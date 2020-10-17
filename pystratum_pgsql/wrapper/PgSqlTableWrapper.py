from typing import Any, Dict

from pystratum_common.wrapper.TableWrapper import TableWrapper

from pystratum_pgsql.wrapper.PgSqlWrapper import PgSqlWrapper


class PgSqlTableWrapper(PgSqlWrapper, TableWrapper):
    """
    Wrapper method generator for printing the result set of stored procedures in a table format.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def _write_result_handler(self, routine: Dict[str, Any]) -> None:
        """
        Generates code of the return statement of the wrapper method for invoking a stored routine.

        :param dict routine: Metadata of the stored routine.
        """
        self._write_line('return self.execute_sp_table({0!s})'.format(self._generate_command(routine)))

# ----------------------------------------------------------------------------------------------------------------------
