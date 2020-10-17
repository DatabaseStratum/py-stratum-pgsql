"""
PyStratum
"""
from test.StratumTestCase import StratumTestCase


class ConstantTest(StratumTestCase):
    # ------------------------------------------------------------------------------------------------------------------
    def test1(self):
        """
        Test constant __ROUTINE__. Must return name of routine.
        """
        row = self._dl.tst_constant01()
        self.assertEqual(row['int'], 1)
        self.assertEqual(row['float'], 1.0)
        self.assertEqual(row['false'], 0)
        self.assertEqual(row['true'], 1)
        self.assertEqual(row['str'], '1')

# ----------------------------------------------------------------------------------------------------------------------
