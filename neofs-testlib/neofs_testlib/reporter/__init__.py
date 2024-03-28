from neofs_testlib.reporter.allure_handler import AllureHandler
from neofs_testlib.reporter.interfaces import ReporterHandler
from neofs_testlib.reporter.reporter import Reporter

__reporter = Reporter()


def get_reporter() -> Reporter:
    """Returns reporter that the library should use for storing artifacts.

    Reporter is a singleton instance that can be configured with multiple handlers that store
    artifacts in various systems. Most common use case is to use single handler.

    Returns:
        Singleton reporter instance.
    """
    return __reporter
