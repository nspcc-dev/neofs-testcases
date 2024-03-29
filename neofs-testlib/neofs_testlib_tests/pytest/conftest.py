def pytest_addoption(parser):
    parser.addoption(
        "--persist-env", action="store_true", default=False, help="persist deployed env"
    )
    parser.addoption("--load-env", action="store", help="load persisted env from file")
