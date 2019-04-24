"""
PyStratum
"""
import sys

from pystratum_test.TestDataLayer import TestDataLayer
from pystratum_test.StratumTestCase import StratumTestCase


class TableTest(StratumTestCase):
    # ------------------------------------------------------------------------------------------------------------------
    def xtest1(self):
        """
        Stored routine with designation type table must show table.
        """
        template_table = """
+---------+---------+---------+---------+---------------------+------+------+
| tst_c00 | tst_c01 | tst_c02 | tst_c03 |       tst_c04       |  t   |  s   |
+---------+---------+---------+---------+---------------------+------+------+
| Hello   |       1 |   0.543 | 1.23450 | 2014-03-27 00:00:00 | 4444 |    1 |
| World   |       3 | 0.00003 | 0.00000 | 2014-03-28 00:00:00 |      |    1 |
+---------+---------+---------+---------+---------------------+------+------+
"""

        TestDataLayer.tst_test_table()
        table = sys.stdout
        self.assertEqual(table, template_table)

# ----------------------------------------------------------------------------------------------------------------------
