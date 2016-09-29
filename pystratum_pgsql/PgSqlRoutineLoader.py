"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
from pystratum.RoutineLoader import RoutineLoader

from pystratum_pgsql.PgSqlMetadataDataLayer import PgSqlMetadataDataLayer
from pystratum_pgsql.PgSqlConnection import PgSqlConnection
from pystratum_pgsql.PgSqlRoutineLoaderHelper import PgSqlRoutineLoaderHelper


class PgSqlRoutineLoader(PgSqlConnection, RoutineLoader):
    """
    Class for loading stored routines into a PostgreSQL instance from (pseudo) SQL files.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, io):
        """
        Object constructor.

        :param pystratum.style.PyStratumStyle.PyStratumStyle io: The output decorator.
        """
        RoutineLoader.__init__(self, io)
        PgSqlConnection.__init__(self, io)

    # ------------------------------------------------------------------------------------------------------------------
    def _get_column_type(self):
        """
        Selects schema, table, column names and the column type from MySQL and saves them as replace pairs.
        """
        rows = PgSqlMetadataDataLayer.get_all_table_columns()
        for row in rows:
            key = '@' + row['table_name'] + '.' + row['column_name'] + '%type@'
            key = key.lower()
            value = row['data_type']

            self._replace_pairs[key] = value

        self._io.text('Selected {0} column types for substitution'.format(len(rows)))

    # ------------------------------------------------------------------------------------------------------------------
    def create_routine_loader_helper(self, routine_name, pystratum_old_metadata, rdbms_old_metadata):
        """
        Creates a Routine Loader Helper object.

        :param str routine_name: The name of the routine.
        :param dict pystratum_old_metadata: The old metadata of the stored routine from PyStratum.
        :param dict rdbms_old_metadata:  The old metadata of the stored routine from PostgreSQL.

        :rtype: pystratum_pgsql.PgSqlRoutineLoaderHelper.PgSqlRoutineLoaderHelper
        """
        return PgSqlRoutineLoaderHelper(self._source_file_names[routine_name],
                                        self._source_file_encoding,
                                        pystratum_old_metadata,
                                        self._replace_pairs,
                                        rdbms_old_metadata,
                                        self._io)

    # ------------------------------------------------------------------------------------------------------------------
    def _get_old_stored_routine_info(self):
        """
        Retrieves information about all stored routines in the current schema.
        """
        rows = PgSqlMetadataDataLayer.get_routines()
        self._rdbms_old_metadata = {}
        for row in rows:
            self._rdbms_old_metadata[row['routine_name']] = row

    # ------------------------------------------------------------------------------------------------------------------
    def _drop_obsolete_routines(self):
        """
        Drops obsolete stored routines (i.e. stored routines that exists in the current schema but for
        which we don't have a source file).
        """
        for routine_name, values in self._rdbms_old_metadata.items():
            if routine_name not in self._source_file_names:
                self._io.text("Dropping {0} <dbo>{1}</dbo>".format(values['routine_type'], routine_name))
                PgSqlMetadataDataLayer.drop_stored_routine(values['routine_type'], routine_name, values['routine_args'])

    # ------------------------------------------------------------------------------------------------------------------
    def _read_configuration_file(self, config_filename):
        """
        Reads parameters from the configuration file.

        :param string config_filename:
        """
        RoutineLoader._read_configuration_file(self, config_filename)
        PgSqlConnection._read_configuration_file(self, config_filename)

# ----------------------------------------------------------------------------------------------------------------------
