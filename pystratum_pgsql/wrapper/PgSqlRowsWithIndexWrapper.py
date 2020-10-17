from typing import Any, Dict

from pystratum_common.wrapper.RowsWithIndexWrapper import RowsWithIndexWrapper

from pystratum_pgsql.wrapper.PgSqlWrapper import PgSqlWrapper


class PgSqlRowsWithIndexWrapper(RowsWithIndexWrapper, PgSqlWrapper):
    """
    Wrapper method generator for stored procedures whose result set  must be returned using tree structure using a
    combination of non-unique columns.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def _write_execute_rows(self, routine: Dict[str, Any]) -> None:
        self._write_line('rows = self.execute_sp_rows({0!s})'.format(self._generate_command(routine)))

# ----------------------------------------------------------------------------------------------------------------------
