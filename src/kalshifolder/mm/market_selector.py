"""
MarketSelector: loads active markets from file with hot-reload support.

- Reads MM_MARKETS_FILE (default: /app/config/markets.txt) every N seconds
- Falls back to MM_MARKETS env var if file missing or empty
- Detects changes (additions/removals) and returns as set
"""

import os
import logging
from typing import Set, Optional
from .utils.time import now_ms

logger = logging.getLogger(__name__)


class MarketSelector:
    def __init__(self, reload_secs: int = 5):
        self.reload_secs = reload_secs
        self.file_path = os.getenv('MM_MARKETS_FILE', '/app/config/markets.txt')
        self.env_fallback = os.getenv('MM_MARKETS', '').strip()
        
        self.last_active_markets: Set[str] = set()
        self.last_reload_ms = 0
        
        # Log startup config
        logger.info(f"MarketSelector initialized: file={self.file_path}, reload_secs={reload_secs}, fallback_count={len(self.env_fallback.split(','))}")

    def get_active_markets(self) -> Set[str]:
        """
        Return current active markets, reloading from file if interval elapsed.
        Returns set of tickers (empty if all invalid).
        """
        now = now_ms()
        if now - self.last_reload_ms < (self.reload_secs * 1000):
            return self.last_active_markets

        # Time to reload
        active = self._read_markets()
        
        if active != self.last_active_markets:
            added = active - self.last_active_markets
            removed = self.last_active_markets - active
            if added or removed:
                logger.info(f"market_set_changed: added={added}, removed={removed}, new_total={len(active)}")
            self.last_active_markets = active

        self.last_reload_ms = now
        return self.last_active_markets

    def _read_markets(self) -> Set[str]:
        """
        Read markets from file, or fall back to env MM_MARKETS if file missing.
        Returns set of valid tickers (blank lines and comments ignored).
        """
        markets = set()

        # Try file first
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        # Skip blank lines and comments
                        if line and not line.startswith('#'):
                            markets.add(line)
                if markets:
                    return markets
            except Exception as e:
                logger.error(f"Failed to read MM_MARKETS_FILE {self.file_path}: {e}")

        # Fall back to env var
        if self.env_fallback:
            for m in self.env_fallback.split(','):
                m = m.strip()
                if m:
                    markets.add(m)
            if markets:
                logger.debug(f"Using MM_MARKETS env fallback ({len(markets)} markets)")
                return markets

        # No valid markets found
        logger.warning("No valid markets found from file or env; trading with empty set")
        return set()
