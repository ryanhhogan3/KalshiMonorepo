import time

def now_ms() -> int:
    return int(time.time() * 1000)


def sleep_ms(ms: int):
    time.sleep(ms / 1000.0)
