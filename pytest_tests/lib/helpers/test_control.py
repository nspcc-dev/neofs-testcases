import logging
from functools import wraps
from time import sleep, time

from _pytest.outcomes import Failed
from pytest import fail

logger = logging.getLogger("NeoLogger")


class expect_not_raises:
    """
    Decorator/Context manager check that some action, method or test does not raises exceptions

    Useful to set proper state of failed test cases in allure

    Example:
        def do_stuff():
            raise Exception("Fail")

        def test_yellow(): <- this test is marked yellow (Test Defect) in allure
            do_stuff()

        def test_red(): <- this test is marked red (Failed) in allure
            with expect_not_raises():
                do_stuff()

        @expect_not_raises()
        def test_also_red(): <- this test is also marked red (Failed) in allure
            do_stuff()
    """

    def __enter__(self):
        pass

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if exception_value:
            fail(str(exception_value))

    def __call__(self, func):
        @wraps(func)
        def impl(*a, **kw):
            with expect_not_raises():
                func(*a, **kw)

        return impl


def wait_for_success(max_wait_time: int = 60, interval: int = 1):
    """
    Decorator to wait for some conditions/functions to pass successfully.
    This is useful if you don't know exact time when something should pass successfully and do not
    want to use sleep(X) with too big X.

    Be careful though, wrapped function should only check the state of something, not change it.
    """

    def wrapper(func):
        @wraps(func)
        def impl(*a, **kw):
            start = int(round(time()))
            last_exception = None
            while start + max_wait_time >= int(round(time())):
                try:
                    return func(*a, **kw)
                except Exception as ex:
                    logger.debug(ex)
                    last_exception = ex
                    sleep(interval)
                except Failed as ex:
                    logger.debug(ex)
                    last_exception = ex
                    sleep(interval)

            # timeout exceeded with no success, raise last_exception
            raise last_exception

        return impl

    return wrapper
