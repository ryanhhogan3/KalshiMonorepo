from __future__ import annotations

import asyncio
import logging
import os
import sys


def _ensure_repo_src_on_path() -> None:
    # This scaffold lives alongside the monorepo. The WS market-data adapter
    # reuses existing repo code under /src, so ensure it's importable.
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    src_dir = os.path.join(repo_root, "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


_ensure_repo_src_on_path()

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
