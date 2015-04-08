import abc
import os
import re
import sys


# ----------------------------------------------------------------------------------------------------------------------
class RoutineLoaderHelper:
    """
    Class for loading a single stored routine into a RDBMS instance from a (pseudo) SQL file.
    """
    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self,
                 routine_filename: str,
                 routine_file_extension: str,
                 routine_file_encoding: str,
                 metadata: dict,
                 replace_pairs: dict,
                 old_routine_info: dict):

        self._source_filename = routine_filename
        """
        The source filename holding the stored routine.

        :type : string
        """

        self._routine_file_extension = routine_file_extension
        """
        The source filename extension.

        :type : string
        """

        self._routine_file_encoding = routine_file_encoding
        """
        The encoding of the routine file.
        """

        self._old_metadata = metadata
        """
        The old metadata of the stored routine.  Note: this data comes from the metadata file.

        :type : dict
        """

        self._metadata = {}
        """
        The metadata of the stored routine. Note: this data is stored in the metadata file and is generated by
        pyStratum.

        :type : dict
        """

        self._replace_pairs = replace_pairs
        """
        A map from placeholders to their actual values.

        :type : dict
        """

        self._old_routine_info = old_routine_info
        """
        The old information about the stored routine. Note: this data comes from the metadata of the RDBMS instance.

        :type : dict
        """

        self._m_time = 0
        """
        The last modification time of the source file.

        :type : int
        """

        self._routine_name = None
        """
        The name of the stored routine.

        :type : string
        """

        self._routine_source_code = None
        """
        The source code as a single string of the stored routine.

        :type : string
        """

        self._routine_source_code_lines = []
        """
        The source code as an array of lines string of the stored routine.

        :type : list
        """

        self._replace = {}
        """
        The replace pairs (i.e. placeholders and their actual values).

        :type : dict
        """

        self._routine_type = None
        """
        The stored routine type (i.e. procedure or function) of the stored routine.

        :type : string
        """

        self._designation_type = None
        """
        The designation type of the stored routine.

        :type : string
        """

        self._columns_types = None
        """
        The column types of columns of the table for bulk insert of the stored routine.

        :type : list
        """

        self._fields = None
        """
        The keys in the dictionary for bulk insert.

        :type : list
        """

        self._parameters = []
        """
        The information about the parameters of the stored routine.

        :type : list
        """

        self._table_name = None
        """
        If designation type is bulk_insert the table name for bulk insert.

        :type : string
        """

        self._columns = None
        """
        The key or index columns (depending on the designation type) of the stored routine .

        :type : list
        """

    # ------------------------------------------------------------------------------------------------------------------
    def load_stored_routine(self) -> dict:
        """
        Loads the stored routine into the instance of MySQL.
        :return array|bool If the stored routine is loaded successfully the new mata data of the stored routine.
                           Otherwise False.
        """
        try:
            self._routine_name = os.path.splitext(os.path.basename(self._source_filename))[0]

            if os.path.exists(self._source_filename):
                if os.path.isfile(self._source_filename):
                    self._m_time = int(os.path.getmtime(self._source_filename))
                else:
                    raise Exception("Unable to get mtime of file '%s'." % self._source_filename)
            else:
                raise Exception("Source file '%s' does not exist." % self._source_filename)

            if self._old_metadata:
                self._metadata = self._old_metadata

            load = self._must_reload()
            if load:
                with open(self._source_filename, 'r', encoding=self._routine_file_encoding) as f:
                    self._routine_source_code = f.read()

                self._routine_source_code_lines = self._routine_source_code.split("\n")

                ok = self._get_placeholders()
                if not ok:
                    return False

                ok = self._get_designation_type()
                if not ok:
                    return False

                ok = self._get_name()
                if not ok:
                    return False

                self._load_routine_file()

                if self._designation_type == 'bulk_insert':
                    self.get_bulk_insert_table_columns_info()

                self._get_routine_parameters_info()

                self._update_metadata()

            return self._metadata

        except Exception as e:
            print('Error', e, file=sys.stderr)
            return False

    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def _must_reload(self) -> bool:
        """
        Returns True if the source file must be load or reloaded. Otherwise returns False.
        :return bool
        """
        pass

    # ------------------------------------------------------------------------------------------------------------------
    def _get_placeholders(self) -> bool:
        """
        Extracts the placeholders from the stored routine source.
        :return True if all placeholders are defined, False otherwise.
        """
        ret = True

        p = re.compile('(@[A-Za-z0-9_\.]+(%type)?@)')
        matches = p.findall(self._routine_source_code)

        placeholders = []

        if len(matches) != 0:
            for tmp in matches:
                placeholder = tmp[0]
                if placeholder.lower() not in self._replace_pairs:
                    print("Error: Unknown placeholder '%s' in file '%s'." % (placeholder, self._source_filename),
                          file=sys.stderr)
                    ret = False
                if placeholder not in placeholders:
                    placeholders.append(placeholder)
        if ret:
            for placeholder in placeholders:
                if placeholder not in self._replace:
                    self._replace['placeholder'] = self._replace_pairs[placeholder.lower()]

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    def _get_designation_type(self) -> bool:
        """
        Extracts the designation type of the stored routine.
        :return True on success. Otherwise returns False.
        """
        pass

    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def _get_name(self) -> bool:
        """
        Extracts the name of the stored routine and the stored routine type (i.e. procedure or function) source.
        :return Returns True on success, False otherwise.
        """
        pass

    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def _load_routine_file(self):
        """
        Loads the stored routine into the RDBMS instance.
        """
        pass

    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_bulk_insert_table_columns_info(self):
        """
        Gets the column names and column types of the current table for bulk insert.
        """
        pass

    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def _get_routine_parameters_info(self):
        pass

    # ------------------------------------------------------------------------------------------------------------------
    def _update_metadata(self):
        """
        Updates the metadata of the stored routine.
        """
        self._metadata['routine_name'] = self._routine_name
        self._metadata['designation'] = self._designation_type
        self._metadata['table_name'] = self._table_name
        self._metadata['parameters'] = self._parameters
        self._metadata['columns'] = self._columns
        self._metadata['fields'] = self._fields
        self._metadata['column_types'] = self._columns_types
        self._metadata['timestamp'] = self._m_time
        self._metadata['replace'] = self._replace

    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def _drop_routine(self):
        """
        Drops the stored routine if it exists.
        """
        pass

    # ------------------------------------------------------------------------------------------------------------------
    def _set_magic_constants(self):
        """
        Adds magic constants to replace list.
        """
        real_path = os.path.realpath(self._source_filename)

        self._replace['__FILE__'] = "'%s'" % real_path
        self._replace['__ROUTINE__'] = "'%s'" % self._routine_name
        self._replace['__DIR__'] = "'%s'" % os.path.dirname(real_path)

    # ------------------------------------------------------------------------------------------------------------------
    def _unset_magic_constants(self):
        """
        Removes magic constants from current replace list.
        """
        if '__FILE__' in self._replace:
            del (self._replace['__FILE__'])

        if '__ROUTINE__' in self._replace:
            del (self._replace['__ROUTINE__'])

        if '__DIR__' in self._replace:
            del (self._replace['__DIR__'])

        if '__LINE__' in self._replace:
            del (self._replace['__LINE__'])


# ----------------------------------------------------------------------------------------------------------------------
