"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
from pystratum.wrapper.RowsWithIndexWrapper import RowsWithIndexWrapper as RowsWithIndexWrapper

from pystratum_pgsql.wrapper.PgSqlWrapper import PgSqlWrapper


class PgSqlRowsWithIndexWrapper(RowsWithIndexWrapper, PgSqlWrapper):
    """
    Wrapper method generator for stored procedures whose result set  must be returned using tree structure using a
    combination of non-unique columns.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def _write_execute_rows(self, routine):
        self._write_line('rows = StaticDataLayer.execute_sp_rows({0!s})'.format(self._generate_command(routine)))

# ----------------------------------------------------------------------------------------------------------------------
