"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
from pystratum.wrapper.LogWrapper import LogWrapper
from pystratum_pgsql.wrapper.PgSqlWrapper import PgSqlWrapper


class PgSqlLogWrapper(PgSqlWrapper, LogWrapper):
    """
    Wrapper method generator for stored procedures with designation type log.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def _write_result_handler(self, routine):
        self._write_line('return StaticDataLayer.execute_sp_log({0!s})'.format(self._generate_command(routine)))

# ----------------------------------------------------------------------------------------------------------------------
