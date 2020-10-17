from configparser import ConfigParser
from typing import Dict, Optional

from pystratum_backend.StratumStyle import StratumStyle
from pystratum_common.backend.CommonRoutineLoaderWorker import CommonRoutineLoaderWorker

from pystratum_pgsql.backend.PgSqlWorker import PgSqlWorker
from pystratum_pgsql.helper.PgSqlRoutineLoaderHelper import PgSqlRoutineLoaderHelper


class PgSqlRoutineLoaderWorker(PgSqlWorker, CommonRoutineLoaderWorker):
    """
    Class for loading stored routines into a PostgreSQL instance from (pseudo) SQL files.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, io: StratumStyle, config: ConfigParser):
        """
        Object constructor.

        :param PyStratumStyle io: The output decorator.
        """
        PgSqlWorker.__init__(self, io, config)
        CommonRoutineLoaderWorker.__init__(self, io, config)

    # ------------------------------------------------------------------------------------------------------------------
    def _get_column_type(self) -> None:
        """
        Selects schema, table, column names and the column type from current database and information_schema and saves
        them as replace pairs.
        """
        rows = self._dl.get_all_table_columns()
        for row in rows:
            key = '@' + row['table_name'] + '.' + row['column_name'] + '%type@'
            key = key.lower()
            value = row['data_type']

            self._replace_pairs[key] = value

        self._io.text('Selected {0} column types for substitution'.format(len(rows)))

    # ------------------------------------------------------------------------------------------------------------------
    def create_routine_loader_helper(self,
                                     routine_name: str,
                                     pystratum_old_metadata: Optional[Dict],
                                     rdbms_old_metadata: Optional[Dict]) -> PgSqlRoutineLoaderHelper:
        """
        Creates a Routine Loader Helper object.

        :param str routine_name: The name of the routine.
        :param dict pystratum_old_metadata: The old metadata of the stored routine from PyStratum.
        :param dict rdbms_old_metadata:  The old metadata of the stored routine from PostgreSQL.

        :rtype: PgSqlRoutineLoaderHelper
        """
        return PgSqlRoutineLoaderHelper(self._io,
                                        self._dl,
                                        self._source_file_names[routine_name],
                                        self._source_file_encoding,
                                        pystratum_old_metadata,
                                        self._replace_pairs,
                                        rdbms_old_metadata)

    # ------------------------------------------------------------------------------------------------------------------
    def _get_old_stored_routine_info(self) -> None:
        """
        Retrieves information about all stored routines in the current schema.
        """
        rows = self._dl.get_routines()
        self._rdbms_old_metadata = {}
        for row in rows:
            self._rdbms_old_metadata[row['routine_name']] = row

    # ------------------------------------------------------------------------------------------------------------------
    def _drop_obsolete_routines(self) -> None:
        """
        Drops obsolete stored routines (i.e. stored routines that exists in the current schema but for
        which we don't have a source file).
        """
        for routine_name, values in self._rdbms_old_metadata.items():
            if routine_name not in self._source_file_names:
                self._io.text("Dropping {0} <dbo>{1}</dbo>".format(values['routine_type'], routine_name))
                self._dl.drop_stored_routine(values['routine_type'], routine_name, values['routine_args'])

# ----------------------------------------------------------------------------------------------------------------------
