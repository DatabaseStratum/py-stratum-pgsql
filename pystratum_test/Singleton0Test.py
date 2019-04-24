"""
PyStratum
"""
from pystratum_test.TestDataLayer import TestDataLayer
from pystratum_test.StratumTestCase import StratumTestCase


class Singleton0Test(StratumTestCase):
    # ------------------------------------------------------------------------------------------------------------------
    def test1(self):
        """
        Stored routine with designation type singleton0 must return null.
        """
        ret = TestDataLayer.tst_test_singleton0a(0)
        self.assertIsNone(ret)

    # ------------------------------------------------------------------------------------------------------------------
    def test2(self):
        """
        Stored routine with designation type singleton0 must return 1 value.
        """
        ret = TestDataLayer.tst_test_singleton0a(1)
        self.assertIsInstance(ret, (str, int, float))

    # ------------------------------------------------------------------------------------------------------------------
    def test3(self):
        """
        An exception must be thrown when a stored routine with designation type singleton0 returns more than 1 values.
        """
        with self.assertRaises(Exception):
            TestDataLayer.tst_test_singleton0a(2)

# ----------------------------------------------------------------------------------------------------------------------
