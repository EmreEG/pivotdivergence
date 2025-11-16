import logging
import os
from datetime import datetime
from typing import Dict, Optional, Tuple

import asyncpg
import numpy as np
import pandas as pd

from config import config


logger = logging.getLogger(__name__)


class BacktestEngine:
    """Run historical simulations of the divergence strategy over persisted market data."""
    def __init__(self, start_date: str, end_date: str, initial_equity: float = 10000):
        self.start_date = start_date
        self.end_date = end_date
        self.initial_equity = initial_equity
        self.current_equity = initial_equity
        
        self.maker_fee = config.backtest['maker_fee']
        self.taker_fee = config.backtest['taker_fee']
        self.funding_interval = config.backtest['funding_interval_s']
        self.reversal_confirm_pct = config.backtest['reversal_confirm_pct']
        self.reversal_confirm_bars = config.backtest['reversal_confirm_bars']
        
        self.trades = []
        self.signals = []
        self.equity_curve = []
        
        self.pool = None
        
    async def initialize(self):
        db_config = config.database
        self.pool = await asyncpg.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            min_size=2,
            max_size=5
        )
        
    async def close(self):
        if self.pool:
            await self.pool.close()
            
    async def load_historical_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        trades_query = '''
            SELECT
                trade_time AS ts,
                price,
                quantity AS qty,
                CASE WHEN is_buyer_maker THEN 'sell' ELSE 'buy' END AS aggressor_side,
                trade_id
            FROM core.binance_trades
            WHERE symbol = $1 AND trade_time >= $2 AND trade_time <= $3
            ORDER BY trade_time
        '''

        book_query = '''
            SELECT ts, bids_json, asks_json
            FROM book_snapshots
            WHERE symbol = $1 AND ts >= $2 AND ts <= $3
            ORDER BY ts
        '''

        mark_query = '''
            SELECT ts, mark_price, funding_rate
            FROM mark_price
            WHERE symbol = $1 AND ts >= $2 AND ts <= $3
            ORDER BY ts
        '''

        oi_query = '''
            SELECT ts, oi_value
            FROM open_interest
            WHERE symbol = $1 AND ts >= $2 AND ts <= $3
            ORDER BY ts
        '''

        start_dt = datetime.fromisoformat(self.start_date)
        end_dt = datetime.fromisoformat(self.end_date)

        # Trades from datapython core.binance_trades
        dp_dsn = os.getenv(
            "DATAPYTHON_DATABASE_DSN",
            "postgresql://postgres:postgres@localhost:5432/market_data",
        )
        dp_conn = await asyncpg.connect(dsn=dp_dsn)
        try:
            trades_rows = await dp_conn.fetch(trades_query, symbol, start_dt, end_dt)
        finally:
            await dp_conn.close()

        # Book / mark / OI from local pivotdivergence schema
        async with self.pool.acquire() as conn:
            book_rows = await conn.fetch(book_query, symbol, start_dt, end_dt)
            mark_rows = await conn.fetch(mark_query, symbol, start_dt, end_dt)
            oi_rows = await conn.fetch(oi_query, symbol, start_dt, end_dt)
            
        trades_df = pd.DataFrame([dict(r) for r in trades_rows])
        book_df = pd.DataFrame([dict(r) for r in book_rows])
        mark_df = pd.DataFrame([dict(r) for r in mark_rows])
        oi_df = pd.DataFrame([dict(r) for r in oi_rows])
        
        data = {
            'trades': trades_df,
            'book': book_df,
            'mark': mark_df,
            'oi': oi_df
        }
        logger.info(
            "Loaded %s trades, %s mark prices for %s",
            len(trades_df),
            len(mark_df),
            symbol,
        )
        return data
        
    def simulate_trade(self, entry_price: float, exit_price: float, 
                      qty_usd: float, side: str, is_maker_entry: bool = True,
                      is_maker_exit: bool = False) -> Dict:
        entry_fee_rate = self.maker_fee if is_maker_entry else self.taker_fee
        exit_fee_rate = self.maker_fee if is_maker_exit else self.taker_fee
        
        if side == 'long':
            pnl_pct = (exit_price - entry_price) / entry_price
        else:
            pnl_pct = (entry_price - exit_price) / entry_price
            
        gross_pnl = qty_usd * pnl_pct
        
        entry_fee = qty_usd * abs(entry_fee_rate)
        exit_fee = qty_usd * abs(exit_fee_rate)
        
        net_pnl = gross_pnl - entry_fee - exit_fee
        
        return {
            'entry_price': entry_price,
            'exit_price': exit_price,
            'qty_usd': qty_usd,
            'side': side,
            'gross_pnl': gross_pnl,
            'entry_fee': entry_fee,
            'exit_fee': exit_fee,
            'net_pnl': net_pnl,
            'return_pct': (net_pnl / qty_usd) * 100
        }
        
    def check_reversal(self, prices: pd.Series, level_price: float, 
                      side: str, touch_idx: int) -> Optional[int]:
        if touch_idx + self.reversal_confirm_bars >= len(prices):
            return None
            
        window = prices.iloc[touch_idx:touch_idx + self.reversal_confirm_bars + 1]
        
        threshold = level_price * self.reversal_confirm_pct
        
        if side == 'long':
            max_price = window.max()
            if max_price >= level_price + threshold:
                reversal_idx = window.idxmax()
                return reversal_idx
        else:
            min_price = window.min()
            if min_price <= level_price - threshold:
                reversal_idx = window.idxmin()
                return reversal_idx
                
        return None
        
    def calculate_metrics(self) -> Dict:
        if not self.trades:
            return {}
            
        trades_df = pd.DataFrame(self.trades)
        
        total_trades = len(trades_df)
        winning_trades = len(trades_df[trades_df['net_pnl'] > 0])
        losing_trades = len(trades_df[trades_df['net_pnl'] < 0])
        
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        
        gross_profit = trades_df[trades_df['net_pnl'] > 0]['net_pnl'].sum()
        gross_loss = abs(trades_df[trades_df['net_pnl'] < 0]['net_pnl'].sum())
        
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
        
        equity_series = pd.Series([self.initial_equity] + [t['equity_after'] for t in self.trades])
        returns = equity_series.pct_change().dropna()
        
        sharpe = 0
        if len(returns) > 0 and returns.std() > 0:
            sharpe = (returns.mean() / returns.std()) * np.sqrt(252)
            
        running_max = equity_series.expanding().max()
        drawdown = (equity_series - running_max) / running_max
        max_drawdown = abs(drawdown.min())
        
        avg_win = trades_df[trades_df['net_pnl'] > 0]['net_pnl'].mean() if winning_trades > 0 else 0
        avg_loss = trades_df[trades_df['net_pnl'] < 0]['net_pnl'].mean() if losing_trades > 0 else 0
        
        total_fees = trades_df['entry_fee'].sum() + trades_df['exit_fee'].sum()
        
        return {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'profit_factor': profit_factor,
            'sharpe_ratio': sharpe,
            'max_drawdown': max_drawdown,
            'total_return': (self.current_equity - self.initial_equity) / self.initial_equity,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'total_fees': total_fees,
            'final_equity': self.current_equity
        }
        
    def calculate_mae_mfe(self, entry_price: float, prices: pd.Series, 
                         side: str, zone_width: float) -> Tuple[float, float]:
        if side == 'long':
            mae = ((prices.min() - entry_price) / zone_width) if zone_width > 0 else 0
            mfe = ((prices.max() - entry_price) / zone_width) if zone_width > 0 else 0
        else:
            mae = ((entry_price - prices.max()) / zone_width) if zone_width > 0 else 0
            mfe = ((entry_price - prices.min()) / zone_width) if zone_width > 0 else 0
            
        return mae, mfe
        
    async def run_backtest(self, symbol: str, strategy_params: Dict = None) -> Dict:
        strategy_params = strategy_params or {}
        await self.initialize()
        
        data = await self.load_historical_data(symbol)
        
        if data['trades'].empty or data['mark'].empty:
            logger.warning("No data available for backtest period for %s", symbol)
            return {}
            
        from analytics.profile import VolumeProfile
        from analytics.extrema import ExtremaDetector
        from analytics.swings import ZigZagSwing
        from analytics.indicators import IndicatorCalculator
        from analytics.naked_poc import NakedPOCTracker
        from analytics.avwap import AVWAPManager
        from analytics.orderflow import CVDCalculator, OBICalculator, OIAnalyzer
        from strategy.level_selector import LevelSelector
        from strategy.divergence import DivergenceDetector
        from strategy.signal_manager import SignalManager
        
        trades_df = data['trades'].copy()
        mark_df = data['mark'].copy()
        trades_df['ts'] = pd.to_datetime(trades_df['ts']).dt.tz_localize(None)
        mark_df['ts'] = pd.to_datetime(mark_df['ts']).dt.tz_localize(None)
        
        profile = VolumeProfile(symbol, 7200)
        extrema_detector = ExtremaDetector()
        swing_detector = ZigZagSwing()
        indicators = IndicatorCalculator()
        NakedPOCTracker()
        AVWAPManager()
        cvd_calc = CVDCalculator()
        OBICalculator()
        OIAnalyzer()
        weight_override = {}
        for weight_key in (config.scoring.get('weights') or {}).keys():
            if weight_key in strategy_params:
                weight_override[weight_key] = strategy_params[weight_key]
        level_selector = LevelSelector(weights_override=weight_override if weight_override else None)
        divergence_detector = DivergenceDetector(
            disable_rsi=strategy_params.get('disable_rsi_divergence', False),
            disable_cvd=strategy_params.get('disable_cvd_divergence', False),
            disable_obi=strategy_params.get('disable_obi_divergence', False)
        )
        SignalManager()
        
        for _, row in trades_df.iterrows():
            profile.add_trade(row['price'], row['qty'], row['ts'].timestamp())
            cvd_calc.process_trade({'amount': row['qty'], 'side': row['aggressor_side'], 'info': {}})
        
        trade_bars_lookup = {}
        if not trades_df.empty:
            resampled = trades_df.set_index('ts').resample('1T').agg(
                open=('price', 'first'),
                high=('price', 'max'),
                low=('price', 'min'),
                close=('price', 'last'),
                volume=('qty', 'sum')
            ).dropna(subset=['close'])
            trade_bars_lookup = {
                ts: {
                    'high': row['high'],
                    'low': row['low'],
                    'volume': row['volume']
                }
                for ts, row in resampled.iterrows()
            }
            
        prices_arr, volumes_arr = profile.get_histogram_array()
        
        if len(prices_arr) == 0:
            logger.warning("No histogram data generated")
            return {}
            
        extrema = extrema_detector.detect_extrema(prices_arr, volumes_arr)
        
        logger.info(
            "Detected %s LVNs, %s HVNs",
            len(extrema['lvns']),
            len(extrema['hvns']),
        )
        
        candidate_levels = []
        for lvn in extrema['lvns']:
            candidate_levels.append({
                'price': lvn['price'],
                'type': 'LVN',
                'prominence': lvn['prominence'],
                'gradient': lvn.get('gradient', 0),
                'window': 'backtest',
                'hours_untouched': 0
            })
            
        naked_pocs = []
        avwaps = {}
        swings = swing_detector.get_last_n_swings(10)
        
        required_confirms = config.divergence['confirmation_count']
        mark_prices = mark_df['mark_price']
        
        for idx, row in mark_df.iterrows():
            current_price = row['mark_price']
            ts = row['ts']
            minute_key = ts.floor('T')
            bar_stats = trade_bars_lookup.get(minute_key, {})
            bar_high = bar_stats.get('high', current_price)
            bar_low = bar_stats.get('low', current_price)
            bar_volume = bar_stats.get('volume', 0.0) or 0.0
            
            indicators.add_bar(current_price, bar_volume, high=bar_high, low=bar_low)
            rsi = indicators.get_rsi()
            realized_vol = indicators.get_realized_volatility()
            cvd_val = cvd_calc.get_cvd()
            swing_detector.update(current_price, ts.timestamp(), rsi, cvd_val, None)
            swings = swing_detector.get_last_n_swings(10)
            naked_pocs = []
            avwaps = {}
            
            long_levels = level_selector.select_levels(
                candidate_levels, current_price, naked_pocs, avwaps, swings, 'long'
            )
            short_levels = level_selector.select_levels(
                candidate_levels, current_price, naked_pocs, avwaps, swings, 'short'
            )
            
            swings_high = swing_detector.get_last_two_highs()
            swings_low = swing_detector.get_last_two_lows()
            
            for level in long_levels[:3]:
                current_price * 0.001
                if abs(current_price - level['price']) / current_price < 0.001:
                    confirmations = divergence_detector.count_confirmations(
                        swings_high,
                        swings_low,
                        None,
                        None,
                        bearish=False,
                        volatility=realized_vol
                    )
                    if confirmations['count'] < required_confirms:
                        continue
                    reversal_idx = self.check_reversal(
                        mark_prices,
                        level['price'],
                        'long',
                        idx
                    )
                    
                    if reversal_idx is not None:
                        entry_price = level['price']
                        exit_price = mark_df.iloc[reversal_idx]['mark_price']
                        
                        qty_usd = self.current_equity * 0.02
                        
                        trade_result = self.simulate_trade(
                            entry_price, exit_price, qty_usd, 'long'
                        )
                        
                        self.current_equity += trade_result['net_pnl']
                        
                        trade_result['equity_after'] = self.current_equity
                        trade_result['level_price'] = level['price']
                        trade_result['level_type'] = level['type']
                        trade_result['divergence_confirms'] = confirmations['count']
                        
                        self.trades.append(trade_result)
                        
                        logger.info(
                            "LONG @ %.2f -> %.2f, PnL=%.2f",
                            entry_price,
                            exit_price,
                            trade_result["net_pnl"],
                        )
            
            for level in short_levels[:3]:
                current_price * 0.001
                if abs(current_price - level['price']) / current_price < 0.001:
                    confirmations = divergence_detector.count_confirmations(
                        swings_high,
                        swings_low,
                        None,
                        None,
                        bearish=True,
                        volatility=realized_vol
                    )
                    if confirmations['count'] < required_confirms:
                        continue
                    reversal_idx = self.check_reversal(
                        mark_prices,
                        level['price'],
                        'short',
                        idx
                    )
                    
                    if reversal_idx is not None:
                        entry_price = level['price']
                        exit_price = mark_df.iloc[reversal_idx]['mark_price']
                        
                        qty_usd = self.current_equity * 0.02
                        
                        trade_result = self.simulate_trade(
                            entry_price, exit_price, qty_usd, 'short'
                        )
                        
                        self.current_equity += trade_result['net_pnl']
                        
                        trade_result['equity_after'] = self.current_equity
                        trade_result['level_price'] = level['price']
                        trade_result['level_type'] = level['type']
                        trade_result['divergence_confirms'] = confirmations['count']
                        
                        self.trades.append(trade_result)
                        
                        logger.info(
                            "SHORT @ %.2f -> %.2f, PnL=%.2f",
                            entry_price,
                            exit_price,
                            trade_result["net_pnl"],
                        )
                        
        metrics = self.calculate_metrics()
        
        await self.close()
        
        return metrics
