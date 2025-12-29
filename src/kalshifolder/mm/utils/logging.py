import logging
import json
import sys


def setup_logging(level=logging.INFO):
    handler = logging.StreamHandler(sys.stdout)
    fmt = '{"ts":"%(asctime)s","level":"%(levelname)s","msg":%(message)s}'
    handler.setFormatter(logging.Formatter(fmt))
    root = logging.getLogger()
    root.handlers = []
    root.addHandler(handler)
    root.setLevel(level)


def json_msg(d: dict) -> str:
    return json.dumps(d)
