"""
StreamerMarketSelector: loads markets from file for streamer with optional extras.

- Reads MM_MARKETS_FILE (shared with MM engine) for base markets
- Falls back to MARKET_TICKERS env var if file missing
- Adds STREAMER_EXTRA_MARKETS if provided (independent streaming)
- No hot-reload loop needed; streamer starts fresh each time
"""

import os
import logging
from typing import Set

logger = logging.getLogger(__name__)


class StreamerMarketSelector:
    def __init__(self):
        self.file_path = os.getenv('MM_MARKETS_FILE', '/app/config/markets.txt')
        self.env_fallback = os.getenv('MARKET_TICKERS', '').strip()
        self.extra_markets = os.getenv('STREAMER_EXTRA_MARKETS', '').strip()
        
    def get_markets(self) -> Set[str]:
        """
        Return markets to stream:
        1. Primary: MM_MARKETS_FILE (shared with MM engine)
        2. Fallback: MARKET_TICKERS env var
        3. Additions: STREAMER_EXTRA_MARKETS env var (optional independent markets)
        
        Returns set of tickers (empty if all invalid).
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
                    logger.info(f"Loaded {len(markets)} markets from {self.file_path}")
            except Exception as e:
                logger.error(f"Failed to read MM_MARKETS_FILE {self.file_path}: {e}")
        
        # Fall back to env var if file missing/empty
        if not markets and self.env_fallback:
            for m in self.env_fallback.split(','):
                m = m.strip()
                if m:
                    markets.add(m)
            if markets:
                logger.info(f"Loaded {len(markets)} markets from MARKET_TICKERS env var")
        
        # Add extra markets for independent streaming
        if self.extra_markets:
            extra_count = 0
            for m in self.extra_markets.split(','):
                m = m.strip()
                if m and m not in markets:
                    markets.add(m)
                    extra_count += 1
            if extra_count:
                logger.info(f"Added {extra_count} extra markets from STREAMER_EXTRA_MARKETS")
        
        if not markets:
            logger.warning("No valid markets found; returning empty set")
        else:
            logger.info(f"Total markets to stream: {len(markets)}")
        
        return markets
