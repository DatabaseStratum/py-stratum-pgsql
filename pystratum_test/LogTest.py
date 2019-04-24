"""
PyStratum
"""
import sys

from pystratum_test.TestDataLayer import TestDataLayer
from pystratum_test.StratumTestCase import StratumTestCase


class LogTest(StratumTestCase):
    # ------------------------------------------------------------------------------------------------------------------
    def test1(self):
        """
        Stored routine with designation type none must return the number of rows affected.
        """
        n = TestDataLayer.tst_test_log()

        self.assertEqual(2, n)

        self.assertRegex(sys.stdout.getvalue(),
                         '^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\sHello, world\n){2}$')

# ----------------------------------------------------------------------------------------------------------------------
