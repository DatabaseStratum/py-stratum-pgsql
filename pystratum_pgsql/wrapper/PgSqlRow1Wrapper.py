"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
from pystratum.wrapper.Row1Wrapper import Row1Wrapper
from pystratum_pgsql.wrapper.PgSqlWrapper import PgSqlWrapper


class PgSqlRow1Wrapper(PgSqlWrapper, Row1Wrapper):
    """
    Wrapper method generator for stored procedures that are selecting 1 row.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def _write_result_handler(self, routine):
        self._write_line('return StaticDataLayer.execute_sp_row1({0!s})'.format(self._generate_command(routine)))

# ----------------------------------------------------------------------------------------------------------------------
