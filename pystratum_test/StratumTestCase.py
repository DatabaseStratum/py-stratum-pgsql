"""
PyStratum
"""
import sys
import unittest
from io import StringIO

from pystratum_test.TestDataLayer import TestDataLayer


class StratumTestCase(unittest.TestCase):
    # ------------------------------------------------------------------------------------------------------------------
    def setUp(self):
        TestDataLayer.connect('localhost', 'test', 'test', 'test', 'test')

        self.held, sys.stdout = sys.stdout, StringIO()

    # ------------------------------------------------------------------------------------------------------------------
    def tearDown(self):
        sys.stdout = self.held
        TestDataLayer.disconnect()

# ----------------------------------------------------------------------------------------------------------------------
