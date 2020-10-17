from configparser import ConfigParser
from typing import Optional

from pystratum_backend.Backend import Backend
from pystratum_backend.ConstantWorker import ConstantWorker
from pystratum_backend.RoutineLoaderWorker import RoutineLoaderWorker
from pystratum_backend.RoutineWrapperGeneratorWorker import RoutineWrapperGeneratorWorker
from pystratum_backend.StratumStyle import StratumStyle

from pystratum_pgsql.backend.PgSqlConstantWorker import PgSqlConstantWorker
from pystratum_pgsql.backend.PgSqlRoutineLoaderWorker import PgSqlRoutineLoaderWorker
from pystratum_pgsql.backend.PgSqlRoutineWrapperGeneratorWorker import PgSqlRoutineWrapperGeneratorWorker


class PgSqlBackend(Backend):
    """
    Semi interface for PyStratum's backends.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def create_constant_worker(self, config: ConfigParser, io: StratumStyle) -> Optional[ConstantWorker]:
        """
        Creates the object that does the actual execution of the constant command for the backend.

        :param ConfigParser config: The settings from the PyStratum configuration file.
        :param StratumStyle io: The output object.

        :rtype: ConstantWorker|None
        """
        return PgSqlConstantWorker(io, config)

    # ------------------------------------------------------------------------------------------------------------------
    def create_routine_loader_worker(self, config: ConfigParser, io: StratumStyle) -> Optional[RoutineLoaderWorker]:
        """
        Creates the object that does the actual execution of the routine loader command for the backend.

        :param ConfigParser config: The settings from the PyStratum configuration file.
        :param StratumStyle io: The output object.

        :rtype: RoutineLoaderWorker|None
        """
        return PgSqlRoutineLoaderWorker(io, config)

    # ------------------------------------------------------------------------------------------------------------------
    def create_routine_wrapper_generator_worker(self, config: ConfigParser, io: StratumStyle) \
            -> Optional[RoutineWrapperGeneratorWorker]:
        """
        Creates the object that does the actual execution of the routine wrapper generator command for the backend.

        :param ConfigParser config: The settings from the PyStratum configuration file.
        :param StratumStyle io: The output object.

        :rtype: RoutineWrapperGeneratorWorker|None
        """
        return PgSqlRoutineWrapperGeneratorWorker(io, config)

# ----------------------------------------------------------------------------------------------------------------------
