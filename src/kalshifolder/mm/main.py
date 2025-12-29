import asyncio
import signal
import logging
from .engine import Engine

logger = logging.getLogger(__name__)


def run():
    engine = Engine()

    loop = asyncio.get_event_loop()

    def _stop(*_):
        engine.stop()

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _stop)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(engine.run())
    finally:
        loop.close()


if __name__ == '__main__':
    run()
