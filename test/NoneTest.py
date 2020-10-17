from test.StratumTestCase import StratumTestCase


class NoneTest(StratumTestCase):
    # ------------------------------------------------------------------------------------------------------------------
    def test1(self):
        """
        Stored routine with designation type none must return the number of rows affected.
        """
        self._dl.execute_none('drop table if exists tmp_tmp')
        self._dl.execute_none('create temporary table tmp_tmp( c int )')
        ret = self._dl.execute_none('insert into tmp_tmp( c ) values(1)')
        self.assertEqual(1, ret, 'insert 1 row')
        ret = self._dl.execute_none('insert into tmp_tmp( c ) values(2), (3)')
        self.assertEqual(2, ret, 'insert 2 rows')
        ret = self._dl.execute_none('delete from tmp_tmp')
        self.assertEqual(3, ret, 'delete 3 rows')

# ----------------------------------------------------------------------------------------------------------------------
