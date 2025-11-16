from typing import Dict, List


class LevelRegistry:
    """Tracks per-level touch history to power age/touch scoring."""

    def __init__(self, tick_size: float, epsilon_pct: float = 0.0015, touch_cooldown_s: int = 60):
        self.tick_size = tick_size
        self.epsilon_pct = epsilon_pct
        self.touch_cooldown_s = touch_cooldown_s
        self.levels: Dict[str, Dict] = {}
        self.expiry_hours = 48

    def set_tick_size(self, tick_size: float):
        self.tick_size = tick_size

    def register_levels(self, levels: List[Dict], timestamp: float):
        for level in levels:
            price = level.get('price')
            if price is None:
                continue
            key = self._key(level)
            existing = self.levels.get(key)
            if not existing:
                self.levels[key] = {
                    'price': float(price),
                    'type': level.get('type'),
                    'window': level.get('window'),
                    'first_seen': timestamp,
                    'last_seen': timestamp,
                    'last_touch': None,
                    'touch_count': 0
                }
            else:
                existing['price'] = float(price)
                existing['last_seen'] = timestamp
        self._cleanup(timestamp)

    def enrich_levels(self, levels: List[Dict], timestamp: float) -> List[Dict]:
        enriched = []
        for level in levels:
            price = level.get('price')
            if price is None:
                enriched.append(level)
                continue
            key = self._key(level)
            meta = self.levels.get(key)
            if not meta:
                enriched.append({**level, 'hours_untouched': 0, 'touch_count': 0})
                continue
            ref_time = meta['last_touch'] or meta['first_seen']
            hours_untouched = max(0.0, (timestamp - ref_time) / 3600)
            enriched.append({
                **level,
                'hours_untouched': hours_untouched,
                'touch_count': meta['touch_count']
            })
        return enriched

    def note_trade(self, price: float, timestamp: float):
        epsilon_pct = self.epsilon_pct or 0.001
        for meta in self.levels.values():
            lvl_price = meta.get('price')
            if lvl_price is None:
                continue
            epsilon = max(lvl_price * epsilon_pct, self.tick_size or 0.0, 1e-6)
            if abs(price - lvl_price) <= epsilon:
                last_touch = meta.get('last_touch')
                if last_touch and (timestamp - last_touch) < self.touch_cooldown_s:
                    continue
                meta['last_touch'] = timestamp
                meta['touch_count'] = meta.get('touch_count', 0) + 1

    def _key(self, level: Dict) -> str:
        price = level.get('price') or 0.0
        rounded_price = round(float(price), 4)
        level_type = level.get('type') or 'GENERIC'
        window = level.get('window') or 'unknown'
        return f"{level_type}:{window}:{rounded_price}"

    def _cleanup(self, timestamp: float):
        expiry = timestamp - self.expiry_hours * 3600
        stale = [
            key for key, meta in self.levels.items()
            if meta.get('last_seen', 0) < expiry
        ]
        for key in stale:
            del self.levels[key]
