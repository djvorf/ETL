import logging
import time
from functools import wraps

logger = logging.getLogger()


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn
    return inner


def retry(exception_to_check: Exception, tries: int = 20, delay: int = 2, backoff: int = 2, logger=logger):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exception_to_check as e:
                    logger.warning(msg=f"{e}, Retrying in {mdelay} seconds...")
                    time.sleep(mdelay)
                    mtries -= 1
                    if mdelay != 256:
                        mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry
    return deco_retry
