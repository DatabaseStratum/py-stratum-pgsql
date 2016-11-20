"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
import os
import re

from pystratum.Constants import Constants
from pystratum.Util import Util
from pystratum_pgsql.PgSqlMetadataDataLayer import PgSqlMetadataDataLayer
from pystratum_pgsql.PgSqlConnection import PgSqlConnection


class PgSqlConstants(PgSqlConnection, Constants):
    """
    Class for creating constants based on column widths, and auto increment columns and labels for PostgreSQL databases.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, io):
        """
        Object constructor.

        :param pystratum.style.PyStratumStyle.PyStratumStyle io: The output decorator.
        """
        Constants.__init__(self, io)
        PgSqlConnection.__init__(self, io)

        self._columns = {}
        """
        All columns in the MySQL schema.

        :type: dict
        """

    # ------------------------------------------------------------------------------------------------------------------
    def _get_old_columns(self):
        """
        Reads from file constants_filename the previous table and column names, the width of the column,
        and the constant name (if assigned) and stores this data in old_columns.
        """
        if os.path.exists(self._constants_filename):
            with open(self._constants_filename, 'r') as stream:
                for line in stream:
                    if line != "\n":
                        prog = re.compile(r'\s*(?:([a-zA-Z0-9_]+)\.)?([a-zA-Z0-9_]+)\.'
                                          r'([a-zA-Z0-9_]+)\s+(\d+)\s*(\*|[a-zA-Z0-9_]+)?\s*')
                        matches = prog.findall(line)

                        if matches:
                            matches = matches[0]
                            schema_name = str(matches[0])
                            table_name = str(matches[1])
                            column_name = str(matches[2])
                            length = str(matches[3])
                            constant_name = str(matches[4])

                            if schema_name:
                                table_name = schema_name + '.' + table_name

                            if constant_name:
                                column_info = {'table_name':    table_name,
                                               'column_name':   column_name,
                                               'length':        length,
                                               'constant_name': constant_name}
                            else:
                                column_info = {'table_name':  table_name,
                                               'column_name': column_name,
                                               'length':      length}

                            if table_name in self._old_columns:
                                if column_name in self._old_columns[table_name]:
                                    pass
                                else:
                                    self._old_columns[table_name][column_name] = column_info
                            else:
                                self._old_columns[table_name] = {column_name: column_info}

    # ------------------------------------------------------------------------------------------------------------------
    def _get_columns(self):
        """
        Retrieves metadata all columns in the MySQL schema.
        """
        rows = PgSqlMetadataDataLayer.get_all_table_columns()
        for row in rows:
            # Enhance row with the actual length of the column.
            row['length'] = self.derive_field_length(row)

            if row['table_name'] in self._columns:
                if row['column_name'] in self._columns[row['table_name']]:
                    pass
                else:
                    self._columns[row['table_name']][row['column_name']] = row
            else:
                self._columns[row['table_name']] = {row['column_name']: row}

    # ------------------------------------------------------------------------------------------------------------------
    def _enhance_columns(self):
        """
        Enhances old_columns as follows:
        If the constant name is *, is is replaced with the column name prefixed by prefix in uppercase.
        Otherwise the constant name is set to uppercase.
        """
        if self._old_columns:
            for table_name, table in sorted(self._old_columns.items()):
                for column_name, column in sorted(table.items()):
                    table_name = column['table_name']
                    column_name = column['column_name']

                    if 'constant_name' in column:
                        if column['constant_name'].strip() == '*':
                            constant_name = str(self._prefix + column['column_name']).upper()
                            self._old_columns[table_name][column_name]['constant_name'] = constant_name
                        else:
                            constant_name = str(self._old_columns[table_name][column_name]['constant_name']).upper()
                            self._old_columns[table_name][column_name]['constant_name'] = constant_name

    # ------------------------------------------------------------------------------------------------------------------
    def _merge_columns(self):
        """
        Preserves relevant data in old_columns into columns.
        """
        if self._old_columns:
            for table_name, table in sorted(self._old_columns.items()):
                for column_name, column in sorted(table.items()):
                    if 'constant_name' in column:
                        try:
                            self._columns[table_name][column_name]['constant_name'] = column['constant_name']
                        except KeyError:
                            # Either the column or table is not present anymore.
                            self._io.warning('Dropping constant {0} because column is not present anymore'.
                                             format(column['constant_name']))

    # ------------------------------------------------------------------------------------------------------------------
    def _write_columns(self):
        """
        Writes table and column names, the width of the column, and the constant name (if assigned) to
        constants_filename.
        """
        content = ''
        for _, table in sorted(self._columns.items()):
            width1 = 0
            width2 = 0

            key_map = {}
            for column_name, column in table.items():
                key_map[column['ordinal_position']] = column_name
                width1 = max(len(str(column['column_name'])), width1)
                width2 = max(len(str(column['length'])), width2)

            for _, column_name in sorted(key_map.items()):
                if table[column_name]['length'] is not None:
                    if 'constant_name' in table[column_name]:
                        line_format = "%s.%-{0:d}s %{1:d}d %s\n".format(int(width1), int(width2))
                        content += line_format % (table[column_name]['table_name'],
                                                  table[column_name]['column_name'],
                                                  table[column_name]['length'],
                                                  table[column_name]['constant_name'])
                    else:
                        line_format = "%s.%-{0:d}s %{1:d}d\n".format(int(width1), int(width2))
                        content += line_format % (table[column_name]['table_name'],
                                                  table[column_name]['column_name'],
                                                  table[column_name]['length'])

            content += "\n"

        # Save the columns, width and constants to the filesystem.
        Util.write_two_phases(self._constants_filename, content, self._io)

    # ------------------------------------------------------------------------------------------------------------------
    def _get_labels(self, regex):
        """
        Gets all primary key labels from the MySQL database.

        :param str regex: The regular expression for columns which we want to use.
        """
        tables = PgSqlMetadataDataLayer.get_label_tables(regex)
        for table in tables:
            rows = PgSqlMetadataDataLayer.get_labels_from_table(table['table_name'], table['id'], table['label'])
            for row in rows:
                self._labels[row['label']] = row['id']

    # ------------------------------------------------------------------------------------------------------------------
    def _fill_constants(self):
        """
        Merges columns and labels (i.e. all known constants) into constants.
        """
        for _, table in sorted(self._columns.items()):
            for _, column in sorted(table.items()):
                if 'constant_name' in column:
                    self._constants[column['constant_name']] = column['length']

        for label, label_id in sorted(self._labels.items()):
            self._constants[label] = label_id

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def derive_field_length(column):
        """
        Returns the width of a field based on the data type of column.

        :param dict column: The column of which the field is based.

        :rtype: int
        """
        types_length = {'bigint':                      21,
                        'integer':                     11,
                        'smallint':                    6,
                        'bit': column                  ['character_maximum_length'],
                        'money':                       None,  # @todo max-length
                        'boolean':                     None,  # @todo max-length
                        'double': column               ['numeric_precision'],
                        'numeric': column              ['numeric_precision'],
                        'real':                        None,  # @todo max-length
                        'character': column            ['character_maximum_length'],
                        'character varying': column    ['character_maximum_length'],
                        'point':                       None,  # @todo max-length
                        'polygon':                     None,  # @todo max-length
                        'text':                        None,  # @todo max-length
                        'bytea':                       None,  # @todo max-length
                        'xml':                         None,  # @todo max-length
                        'USER-DEFINED':                None,
                        'timestamp without time zone': 16,
                        'time without time zone':      8,
                        'date':                        10}

        if column['data_type'] in types_length:
            return types_length[column['data_type']]

        raise Exception("Unexpected type '{0!s}'.".format(column['data_type']))

    # ------------------------------------------------------------------------------------------------------------------
    def _read_configuration_file(self, config_filename):
        """
        Reads parameters from the configuration file.

        :param str config_filename:
        """
        Constants._read_configuration_file(self, config_filename)
        PgSqlConnection._read_configuration_file(self, config_filename)

# ----------------------------------------------------------------------------------------------------------------------
