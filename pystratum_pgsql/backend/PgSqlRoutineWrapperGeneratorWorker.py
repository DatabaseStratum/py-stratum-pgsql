from configparser import ConfigParser
from typing import Dict

from pystratum_backend.StratumStyle import StratumStyle
from pystratum_common.backend.CommonRoutineWrapperGeneratorWorker import CommonRoutineWrapperGeneratorWorker

from pystratum_pgsql.backend.PgSqlWorker import PgSqlWorker
from pystratum_pgsql.wrapper import create_routine_wrapper


class PgSqlRoutineWrapperGeneratorWorker(PgSqlWorker, CommonRoutineWrapperGeneratorWorker):
    """
    Class for generating a class with wrapper methods for calling stored routines in a PostgreSQL database.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, io: StratumStyle, config: ConfigParser):
        """
        Object constructor.

        :param PyStratumStyle io: The output decorator.
        """
        PgSqlWorker.__init__(self, io, config)
        CommonRoutineWrapperGeneratorWorker.__init__(self, io, config)

    # ------------------------------------------------------------------------------------------------------------------
    def _write_routine_function(self, routine: Dict) -> None:
        """
        Generates a complete wrapper method for a stored routine.

        :param dict routine: The metadata of the stored routine.
        """
        wrapper = create_routine_wrapper(routine, self._lob_as_string_flag)
        self._code += wrapper.write_routine_method(routine)

# ----------------------------------------------------------------------------------------------------------------------
