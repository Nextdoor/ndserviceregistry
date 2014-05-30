from kazoo.testing import KazooTestHarness
from nd_service_registry import KazooServiceRegistry


class KazooServiceRegistryIntegrationTests(KazooTestHarness):
    # A flag for filtering nose tests
    integration = True

    def setUp(self):
        self.setup_zookeeper()
        self.server = 'localhost:20000'
        self.ndsr = KazooServiceRegistry(server=self.server,
                                         rate_limit_calls=0,
                                         rate_limit_time=0)

    def tearDown(self):
        self.teardown_zookeeper()

    def test_get_state(self):
        self.ndsr.start()
        self.assertTrue(self.ndsr.get_state())

        self.ndsr.stop()
        self.assertFalse(self.ndsr.get_state())
