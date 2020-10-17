import sys
import unittest
from io import StringIO

from pystratum_pgsql.PgSqlDefaultConnector import PgSqlDefaultConnector
from test.TestDataLayer import TestDataLayer


class StratumTestCase(unittest.TestCase):
    """
    Test case with a data layer.
    """

    def __init__(self, method_name):
        """
        Object constructor.
        """
        super().__init__(method_name)

        params = {'host':     '127.0.0.1',
                  'user':     'test',
                  'password': 'test',
                  'database': 'test',
                  'schema':   'test',
                  'port':     5432}

        self._dl = TestDataLayer(PgSqlDefaultConnector(params))
        """
        The generated data layer.
        """

    # ------------------------------------------------------------------------------------------------------------------
    def setUp(self):
        self._dl.connect()

        self.held, sys.stdout = sys.stdout, StringIO()

    # ------------------------------------------------------------------------------------------------------------------
    def tearDown(self):
        sys.stdout = self.held
        self._dl.disconnect()

# ----------------------------------------------------------------------------------------------------------------------
