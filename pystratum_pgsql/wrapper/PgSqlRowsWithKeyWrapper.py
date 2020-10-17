from typing import Any, Dict

from pystratum_common.wrapper.RowsWithKeyWrapper import RowsWithKeyWrapper

from pystratum_pgsql.wrapper.PgSqlWrapper import PgSqlWrapper


class PgSqlRowsWithKeyWrapper(RowsWithKeyWrapper, PgSqlWrapper):
    """
    Wrapper method generator for stored procedures whose result set must be returned using tree structure using a
    combination of unique columns.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def _write_execute_rows(self, routine: Dict[str, Any]):
        self._write_line('rows = self.execute_sp_rows({0!s})'.format(self._generate_command(routine)))

# ----------------------------------------------------------------------------------------------------------------------
