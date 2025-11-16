import asyncio
import logging
import numpy as np
from typing import Dict, List
from itertools import product
from backtest.engine import BacktestEngine
from config import config


logger = logging.getLogger(__name__)


class Optimizer:
    def __init__(self, start_date: str, end_date: str):
        """Walk-forward optimizer for divergence strategy hyperparameters."""
        self.start_date = start_date
        self.end_date = end_date
        self.train_months = config.backtest['walk_forward_train_months']
        self.test_months = config.backtest['walk_forward_test_months']
        
    def generate_param_grid(self) -> List[Dict]:
        param_space = {
            'lvn_prominence': [1.0, 1.5, 2.0],
            'confluence': [1, 2, 3],
            'naked_poc': [1, 2, 3],
            'shape_bias': [0.5, 1.0, 1.5],
            'distance': [0.5, 1.0, 1.5],
            'untouched_age': [0.5, 1.0, 1.5],
            'avwap_confluence': [0.5, 1.0, 1.5]
        }
        
        keys = param_space.keys()
        values = param_space.values()
        
        combinations = list(product(*values))
        
        param_grid = [dict(zip(keys, combo)) for combo in combinations]
        
        return param_grid
        
    async def evaluate_params(self, params: Dict, symbol: str, 
                             start_date: str, end_date: str) -> Dict:
        engine = BacktestEngine(start_date, end_date)
        
        metrics = await engine.run_backtest(symbol, params)
        
        if not metrics:
            return {
                'params': params,
                'sharpe': -999,
                'max_drawdown': 1.0,
                'total_return': -1.0
            }
            
        return {
            'params': params,
            'sharpe': metrics.get('sharpe_ratio', -999),
            'max_drawdown': metrics.get('max_drawdown', 1.0),
            'total_return': metrics.get('total_return', -1.0),
            'win_rate': metrics.get('win_rate', 0),
            'profit_factor': metrics.get('profit_factor', 0),
            'total_trades': metrics.get('total_trades', 0)
        }
        
    def is_valid_result(self, result: Dict) -> bool:
        target_sharpe = config.backtest['target_sharpe']
        max_dd = config.backtest['max_drawdown']
        
        return (result['sharpe'] >= target_sharpe and 
                result['max_drawdown'] <= max_dd and
                result['total_trades'] >= 10)
                
    async def walk_forward_optimize(self, symbol: str, ablation_mode: str = None) -> List[Dict]:
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        
        start_dt = datetime.fromisoformat(self.start_date)
        end_dt = datetime.fromisoformat(self.end_date)
        
        results = []
        current_start = start_dt
        
        while current_start < end_dt:
            train_end = current_start + relativedelta(months=self.train_months)
            test_end = train_end + relativedelta(months=self.test_months)
            
            if test_end > end_dt:
                break
                
            logger.info(
                "Walk-forward period: train %s to %s, test %s to %s",
                current_start.date(),
                train_end.date(),
                train_end.date(),
                test_end.date(),
            )
            
            if ablation_mode:
                logger.info("Ablation mode: %s", ablation_mode)
            
            param_grid = self.generate_param_grid()
            
            if ablation_mode:
                param_grid = self.apply_ablation(param_grid, ablation_mode)
            
            logger.info(
                "Testing %s parameter combinations",
                len(param_grid),
            )
            
            train_results = []
            for i, params in enumerate(param_grid[:10]):
                result = await self.evaluate_params(
                    params,
                    symbol,
                    current_start.isoformat(),
                    train_end.isoformat()
                )
                train_results.append(result)
                
                if (i + 1) % 5 == 0:
                    logger.info(
                        "Evaluated %s/%s combinations",
                        i + 1,
                        len(param_grid[:10]),
                    )
                    
            valid_results = [r for r in train_results if self.is_valid_result(r)]
            
            if not valid_results:
                logger.warning(
                    "No valid parameter sets found for training period starting %s",
                    current_start.date(),
                )
                current_start = test_end
                continue
                
            best_train = max(valid_results, key=lambda x: x['sharpe'])
            
            logger.info("Best train Sharpe: %.2f", best_train["sharpe"])
            
            test_result = await self.evaluate_params(
                best_train['params'],
                symbol,
                train_end.isoformat(),
                test_end.isoformat()
            )
            
            logger.info("Test Sharpe: %.2f", test_result["sharpe"])
            logger.info("Test Max DD: %.2f%%", test_result["max_drawdown"] * 100)
            logger.info("Test Return: %.2f%%", test_result["total_return"] * 100)
            
            results.append({
                'train_start': current_start.isoformat(),
                'train_end': train_end.isoformat(),
                'test_start': train_end.isoformat(),
                'test_end': test_end.isoformat(),
                'best_params': best_train['params'],
                'train_sharpe': best_train['sharpe'],
                'test_sharpe': test_result['sharpe'],
                'test_max_dd': test_result['max_drawdown'],
                'test_return': test_result['total_return'],
                'test_trades': test_result['total_trades'],
                'ablation': ablation_mode
            })
            
            current_start = test_end
            
        return results
        
    def apply_ablation(self, param_grid: List[Dict], ablation_mode: str) -> List[Dict]:
        if ablation_mode == 'no_lvn':
            for params in param_grid:
                params['lvn_prominence'] = 0
        elif ablation_mode == 'no_naked_poc':
            for params in param_grid:
                params['naked_poc'] = 0
        elif ablation_mode == 'no_avwap':
            for params in param_grid:
                params['avwap_confluence'] = 0
        elif ablation_mode == 'no_cvd':
            # Disable CVD divergence checks in strategy
            for params in param_grid:
                params['disable_cvd_divergence'] = True
        elif ablation_mode == 'no_obi':
            # Disable OBI divergence checks in strategy
            for params in param_grid:
                params['disable_obi_divergence'] = True
        elif ablation_mode == 'no_rsi':
            # Disable RSI divergence checks in strategy
            for params in param_grid:
                params['disable_rsi_divergence'] = True
            
        return param_grid
        
    def summarize_walk_forward(self, results: List[Dict]) -> Dict:
        if not results:
            return {}
            
        test_sharpes = [r['test_sharpe'] for r in results if r['test_sharpe'] > -999]
        test_returns = [r['test_return'] for r in results]
        test_dds = [r['test_max_dd'] for r in results]
        
        return {
            'num_periods': len(results),
            'avg_test_sharpe': np.mean(test_sharpes) if test_sharpes else 0,
            'std_test_sharpe': np.std(test_sharpes) if test_sharpes else 0,
            'avg_test_return': np.mean(test_returns),
            'avg_test_max_dd': np.mean(test_dds),
            'total_test_return': np.prod([1 + r for r in test_returns]) - 1,
            'periods': results
        }

async def run_optimization(symbol: str, start_date: str, end_date: str, ablation_mode: str = None):
    optimizer = Optimizer(start_date, end_date)
    
    results = await optimizer.walk_forward_optimize(symbol, ablation_mode)
    
    summary = optimizer.summarize_walk_forward(results)
    
    logger.info("WALK-FORWARD OPTIMIZATION SUMMARY")
    if ablation_mode:
        logger.info("Ablation mode: %s", ablation_mode)
    logger.info("Periods tested: %s", summary["num_periods"])
    logger.info("Avg test Sharpe: %.2f", summary["avg_test_sharpe"])
    logger.info("Avg test return: %.2f%%", summary["avg_test_return"] * 100)
    logger.info("Avg test max DD: %.2f%%", summary["avg_test_max_dd"] * 100)
    logger.info("Total test return: %.2f%%", summary["total_test_return"] * 100)
    
    return summary

async def run_full_ablation_study(symbol: str, start_date: str, end_date: str):
    ablation_modes = [None, 'no_lvn', 'no_naked_poc', 'no_avwap', 'no_cvd', 'no_obi', 'no_rsi']
    
    all_results = {}
    
    for mode in ablation_modes:
        mode_name = mode if mode else 'baseline'
        logger.info("Running ablation study: %s", mode_name)
        
        summary = await run_optimization(symbol, start_date, end_date, mode)
        all_results[mode_name] = summary
        
    for mode_name, summary in all_results.items():
        logger.info(
            "%s: Avg Sharpe %.2f, Avg Return %.2f%%, Avg Max DD %.2f%%",
            mode_name,
            summary.get("avg_test_sharpe", 0),
            summary.get("avg_test_return", 0) * 100,
            summary.get("avg_test_max_dd", 0) * 100,
        )
        
    return all_results

if __name__ == "__main__":
    asyncio.run(run_optimization('BTCUSDT', '2024-01-01', '2024-12-31'))
