"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
import re

from psycopg2 import ProgrammingError

from pystratum.RoutineLoaderHelper import RoutineLoaderHelper
from pystratum_pgsql.PgSqlMetadataDataLayer import PgSqlMetadataDataLayer
from pystratum_pgsql.helper.PgSqlDataTypeHelper import PgSqlDataTypeHelper


class PgSqlRoutineLoaderHelper(RoutineLoaderHelper):
    """
    Class for loading a single stored routine into a PostgreSQL instance from a (pseudo) SQL file.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def _must_reload(self):
        """
        Returns True if the source file must be load or reloaded. Otherwise returns False.

        :rtype: bool
        """
        if not self._pystratum_old_metadata:
            return True

        if self._pystratum_old_metadata['timestamp'] != self._m_time:
            return True

        if self._pystratum_old_metadata['replace']:
            for key, value in self._pystratum_old_metadata['replace'].items():
                if key.lower() not in self._replace_pairs or self._replace_pairs[key.lower()] != value:
                    return True

        if not self._rdbms_old_metadata:
            return True

        return False

    # ------------------------------------------------------------------------------------------------------------------
    def _get_name(self):
        """
        Extracts the name of the stored routine and the stored routine type (i.e. procedure or function) source.
        Returns True on success, False otherwise.

        :rtype: bool
        """
        ret = True
        prog = re.compile("create\\s+(function)\\s+([a-zA-Z0-9_]+)")
        matches = prog.findall(self._routine_source_code)

        if matches:
            self._routine_type = matches[0][0].lower()

            if self._routine_name != matches[0][1]:
                self._io.error('Stored routine name <dbo>{0}</dbo> does not match filename in file <fso>{1}</fso>'.
                               format(matches[0][1], self._source_filename))
                ret = False
        else:
            ret = False

        if not self._routine_type:
            self._io.error('Unable to find the stored routine name and type in file <fso>{0}</fso>'.
                           format(self._source_filename))

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    def _get_data_type_helper(self):
        """
        Returns a data type helper object or PostgreSQL.

        :rtype: pystratum.helper.DataTypeHelper.DataTypeHelper
        """
        return PgSqlDataTypeHelper()

    # ------------------------------------------------------------------------------------------------------------------
    def _is_start_or_store_routine(self, line):
        """
        Returns True if a line is the start of the code of the stored routine.

        :param str line: The line with source code of the stored routine.

        :rtype: bool
        """
        return re.match(r'^\s*create\s+(function)', line) is not None

    # ------------------------------------------------------------------------------------------------------------------
    def _load_routine_file(self):
        """
        Loads the stored routine into the MySQL instance.
        """
        self._io.text('Loading {0} <dbo>{1}</dbo>'.format(self._routine_type, self._routine_name))

        self._set_magic_constants()

        routine_source = []
        i = 0
        for line in self._routine_source_code_lines:
            new_line = line
            self._replace['__LINE__'] = "'%d'" % (i + 1)
            for search, replace in self._replace.items():
                tmp = re.findall(search, new_line, re.IGNORECASE)
                if tmp:
                    new_line = new_line.replace(tmp[0], replace)
            routine_source.append(new_line)
            i += 1

        routine_source = "\n".join(routine_source)

        self._unset_magic_constants()
        self._drop_routine()

        PgSqlMetadataDataLayer.commit()
        PgSqlMetadataDataLayer.execute_none(routine_source)
        PgSqlMetadataDataLayer.commit()

    # ------------------------------------------------------------------------------------------------------------------
    def _log_exception(self, exception):
        """
        Logs an exception.

        :param Exception exception: The exception.
        """
        PgSqlMetadataDataLayer.rollback()

        RoutineLoaderHelper._log_exception(self, exception)

        if isinstance(exception, ProgrammingError):
            if 'syntax error at or near' in str(exception):
                cursor = exception.cursor
                if cursor:
                    sql = str(cursor.query, 'utf-8').rstrip()

                    parts = re.search(r'LINE (\d+):', str(exception))
                    if parts:
                        error_line = int(parts.group(1))
                    else:
                        error_line = 0

                    self._print_sql_with_error(sql, error_line)

    # ------------------------------------------------------------------------------------------------------------------
    def get_bulk_insert_table_columns_info(self):
        """
        Gets the column names and column types of the current table for bulk insert.
        """
        table_is_non_temporary = PgSqlMetadataDataLayer.check_table_exists(self._table_name)

        if not table_is_non_temporary:
            PgSqlMetadataDataLayer.call_stored_routine(self._routine_name)

        columns = PgSqlMetadataDataLayer.describe_table(self._table_name)

        tmp_column_types = []
        tmp_fields = []

        n1 = 0
        for column in columns:
            c_type = re.findall(r'(\\w+)', column['Type'])
            tmp_column_types.append(c_type[0])
            tmp_fields.append(column['Field'])
            n1 += 1

        n2 = len(self._columns)

        if not table_is_non_temporary:
            PgSqlMetadataDataLayer.drop_temporary_table(self._table_name)

        if n1 != n2:
            raise Exception("Number of fields %d and number of columns %d don't match." % (n1, n2))

        self._columns_types = tmp_column_types
        self._fields = tmp_fields

    # ------------------------------------------------------------------------------------------------------------------
    def _get_designation_type(self):
        """
        Extracts the designation type of the stored routine. Returns True on success. Otherwise returns False.

        :rtype: bool
        """
        ret = True

        key = self._routine_source_code_lines.index('begin')

        if key != -1:
            matches = re.findall(r'\s*--\s+type:\s*(\w+)\s*(.+)?\s*', self._routine_source_code_lines[key - 1])

            if matches:
                self._designation_type = matches[0][0]
                tmp = str(matches[0][1])
                if self._designation_type == 'bulk_insert':
                    info = re.findall(r'([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_,]+)', tmp)

                    if not info:
                        self._io.error('Expected: -- type: bulk_insert <table_name> <columns> in file <fso>{0}</fso>'.
                                       format(self._source_filename))
                    self._table_name = info[0][0]
                    self._columns = str(info[0][1]).split(',')

                elif self._designation_type == 'rows_with_key' or self._designation_type == 'rows_with_index':
                    self._columns = str(matches[0][1]).split(',')
                else:
                    if matches[0][1]:
                        ret = False
        else:
            ret = False

        if not ret:
            self._io.error("Unable to find the designation type of the stored routine in file <fso>{0}</fso>".
                           format(self._source_filename))

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    def _get_routine_parameters_info(self):
        """
        Retrieves information about the stored routine parameters from the meta data of PostgreSQL.
        """
        routine_parameters = PgSqlMetadataDataLayer.get_routine_parameters(self._routine_name)
        for routine_parameter in routine_parameters:
            if routine_parameter['parameter_name']:
                value = routine_parameter['column_type']

                self._parameters.append({'name': routine_parameter     ['parameter_name'],
                                         'data_type': routine_parameter['parameter_type'],
                                         'data_type_descriptor':       value})

    # ------------------------------------------------------------------------------------------------------------------
    def _drop_routine(self):
        """
        Drops the stored routine if it exists.
        """
        if self._rdbms_old_metadata:
            PgSqlMetadataDataLayer.drop_stored_routine(self._rdbms_old_metadata['routine_type'],
                                                       self._routine_name,
                                                       self._rdbms_old_metadata['routine_args'])

# ----------------------------------------------------------------------------------------------------------------------
