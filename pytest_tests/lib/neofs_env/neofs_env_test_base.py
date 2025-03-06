import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from neofs_testlib.env.env import NeoFSEnv


class NeofsEnvTestBase:
    def tick_epoch(self):
        neofs_epoch.tick_epoch(self.neofs_env)

    def get_epoch(self):
        return neofs_epoch.get_epoch(self.neofs_env)

    def ensure_fresh_epoch(self):
        return neofs_epoch.ensure_fresh_epoch(self.neofs_env)

    @allure.step("Tick epochs and wait for epoch alignment")
    def tick_epochs_and_wait(self, epochs_to_tick: int):
        current_epoch = self.get_epoch()
        for _ in range(epochs_to_tick):
            self.tick_epoch()
            neofs_epoch.wait_for_epochs_align(self.neofs_env, current_epoch)


class TestNeofsBase(NeofsEnvTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def fill_mandatory_dependencies(self, request, neofs_env: NeoFSEnv):
        request.cls.shell = neofs_env.shell
        request.cls.neofs_env = neofs_env
        yield
