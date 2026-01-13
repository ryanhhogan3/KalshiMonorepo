"""
MarketSelector: loads active markets from file with hot-reload support.

- Reads MM_MARKETS_FILE (default: /app/config/markets.txt) every N seconds
- Single source of truth: file only (no env var fallback)
"""

import os
import logging
import json
from typing import Set, Optional
from .utils.time import now_ms

logger = logging.getLogger(__name__)


class MarketSelector:
    def __init__(self, reload_secs: int = 5):
        self.reload_secs = reload_secs
        self.file_path = os.getenv('MM_MARKETS_FILE', '/app/config/markets.txt')
        
        self.last_active_markets: Set[str] = set()
        self.last_reload_ms = 0
        
        # Log startup config
        logger.info(json.dumps({
            "event": "market_selector_init",
            "file": self.file_path,
            "reload_secs": reload_secs
        }))

    def get_active_markets(self) -> Set[str]:
        """
        Return current active markets, reloading from file if interval elapsed.
        Returns set of tickers (empty if all invalid or file missing).
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
                logger.info(json.dumps({
                    "event": "markets_reload",
                    "added": sorted(list(added)),
                    "removed": sorted(list(removed)),
                    "active_count": len(active)
                }))
            self.last_active_markets = active

        self.last_reload_ms = now
        return self.last_active_markets

    def _read_markets(self) -> Set[str]:
        """
        Read markets from file only (single source of truth).
        Returns set of valid tickers (blank lines and comments ignored).
        Returns empty set if file missing.
        """
        markets = set()

        # File only - no env var fallback
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        # Skip blank lines and comments
                        if line and not line.startswith('#'):
                            markets.add(line)
                return markets
            except Exception as e:
                logger.error(json.dumps({
                    "event": "markets_file_read_error",
                    "file_path": self.file_path,
                    "error": str(e)
                }))
        else:
            logger.warning(json.dumps({
                "event": "markets_file_missing",
                "file_path": self.file_path
            }))

        return markets
        # No valid markets found
        logger.warning("No valid markets found from file or env; trading with empty set")
        return set()
