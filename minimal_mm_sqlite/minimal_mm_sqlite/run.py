from __future__ import annotations

import asyncio
import logging
import sys

from .engine import Engine


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> int:
    _configure_logging()
    try:
        engine = Engine.build()
    except Exception as e:
        logging.getLogger(__name__).error("failed_to_build_engine err=%r", e)
        return 1

    try:
        asyncio.run(engine.run_forever())
        return 0
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
