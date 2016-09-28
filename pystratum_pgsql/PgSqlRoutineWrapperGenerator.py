"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
from pystratum.RoutineWrapperGenerator import RoutineWrapperGenerator

from pystratum_pgsql.PgSqlConnection import PgSqlConnection
from pystratum_pgsql.wrapper import create_routine_wrapper


class PgSqlRoutineWrapperGenerator(PgSqlConnection, RoutineWrapperGenerator):
    """
    Class for generating a class with wrapper methods for calling stored routines in a PostgreSQL database.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, io):
        """
        Object constructor.

        :param pystratum.style.PyStratumStyle.PyStratumStyle io: The output decorator.
        """
        PgSqlConnection.__init__(self, io)
        RoutineWrapperGenerator.__init__(self, io)

    # ------------------------------------------------------------------------------------------------------------------
    def _read_configuration_file(self, config_filename):
        """
        Reads parameters from the configuration file.

        :param str config_filename: The name of the configuration file.
        """
        PgSqlConnection._read_configuration_file(self, config_filename)
        RoutineWrapperGenerator._read_configuration_file(self, config_filename)

    # ------------------------------------------------------------------------------------------------------------------
    def _write_routine_function(self, routine):
        """
        Generates a complete wrapper method for a stored routine.

        :param dict routine: The metadata of the stored routine.
        """
        wrapper = create_routine_wrapper(routine, self._lob_as_string_flag)
        self._code += wrapper.write_routine_method(routine)

# ----------------------------------------------------------------------------------------------------------------------
