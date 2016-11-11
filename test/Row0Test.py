"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
from test.TestDataLayer import TestDataLayer
from test.StratumTestCase import StratumTestCase


class Row0Test(StratumTestCase):
    # ------------------------------------------------------------------------------------------------------------------
    def test1(self):
        """
        Stored routine with designation type row0 must return null.
        """
        ret = TestDataLayer.tst_test_row0a(0)
        self.assertIsNone(ret)

    # ------------------------------------------------------------------------------------------------------------------
    def test2(self):
        """
        Stored routine with designation type row0 must return 1 row.
        """
        ret = TestDataLayer.tst_test_row0a(1)
        self.assertIsInstance(ret, dict)

    # ------------------------------------------------------------------------------------------------------------------
    def test3(self):
        """
        An exception must be thrown when a stored routine with designation type row0 returns more than 1 rows.
        @expectedException Exception
        """
        with self.assertRaises(Exception):
            TestDataLayer.tst_test_row0a(2)

    # ------------------------------------------------------------------------------------------------------------------
    def test4(self):
        """
        Test column tst_c00 is selected correctly.
        """
        ret = TestDataLayer.tst_test_row0a(1)
        self.assertEqual(1, ret['tst_c00'])

# ----------------------------------------------------------------------------------------------------------------------
