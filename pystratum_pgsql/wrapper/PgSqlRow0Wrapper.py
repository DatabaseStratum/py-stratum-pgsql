"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
from pystratum.wrapper.Row0Wrapper import Row0Wrapper
from pystratum_pgsql.wrapper.PgSqlWrapper import PgSqlWrapper


class PgSqlRow0Wrapper(PgSqlWrapper, Row0Wrapper):
    """
    Wrapper method generator for stored procedures that are selecting 0 or 1 row.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def _write_result_handler(self, routine):
        self._write_line('return StaticDataLayer.execute_sp_row0({0!s})'.format(self._generate_command(routine)))

# ----------------------------------------------------------------------------------------------------------------------
