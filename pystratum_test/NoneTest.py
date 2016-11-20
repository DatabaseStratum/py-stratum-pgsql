"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""

from pystratum_test.TestDataLayer import TestDataLayer
from pystratum_test.StratumTestCase import StratumTestCase


class NoneTest(StratumTestCase):
    # ------------------------------------------------------------------------------------------------------------------
    def test1(self):
        """
        Stored routine with designation type none must return the number of rows affected.
        """
        TestDataLayer.execute_none('drop table if exists tmp_tmp')
        TestDataLayer.execute_none('create temporary table tmp_tmp( c int )')
        ret = TestDataLayer.execute_none('insert into tmp_tmp( c ) values(1)')
        self.assertEqual(1, ret, 'insert 1 row')
        ret = TestDataLayer.execute_none('insert into tmp_tmp( c ) values(2), (3)')
        self.assertEqual(2, ret, 'insert 2 rows')
        ret = TestDataLayer.execute_none('delete from tmp_tmp')
        self.assertEqual(3, ret, 'delete 3 rows')

# ----------------------------------------------------------------------------------------------------------------------
