"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
import os

from pystratum_test.TestDataLayer import TestDataLayer
from pystratum_test.StratumTestCase import StratumTestCase


class ConstantTest(StratumTestCase):
    # ------------------------------------------------------------------------------------------------------------------
    def test1(self):
        """
        Test constant __ROUTINE__. Must return name of routine.
        """
        row = TestDataLayer.tst_constant01()
        self.assertEqual(row['int'], 1)
        self.assertEqual(row['float'], 1.0)
        self.assertEqual(row['false'], 0)
        self.assertEqual(row['true'], 1)
        self.assertEqual(row['str'], '1')

# ----------------------------------------------------------------------------------------------------------------------
