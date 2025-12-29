import time
import functools

def retry(times: int = 3, delay_s: float = 0.5):
    def deco(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            last = None
            for i in range(times):
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    last = e
                    time.sleep(delay_s)
            raise last
        return wrapped
    return deco
