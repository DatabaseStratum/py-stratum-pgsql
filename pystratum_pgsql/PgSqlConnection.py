"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
from pystratum.Connection import Connection
from pystratum.MetadataDataLayer import MetadataDataLayer

from pystratum_pgsql.PgSqlMetadataDataLayer import PgSqlMetadataDataLayer


class PgSqlConnection(Connection):
    """
    Class for connecting to PostgreSQL instances and reading PostgreSQL specific connection parameters from 
    configuration files.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, io):
        """
        Object constructor.

        :param pystratum.style.PyStratumStyle.PyStratumStyle io: The output decorator.
        """
        Connection.__init__(self, io)

        self._host = 'localhost'
        """
        The hostname of the PostgreSQL instance.

        :type: str
        """

        self._port = 5432
        """
        The port of the PostgreSQL instance.

        :type: int
        """

        self._user = ''
        """
        User name.

        :type: str
        """

        self._password = ''
        """
        Password required for logging in on to the PostgreSQL instance.

        :type: str
        """

        self._database = ''
        """
        The database name.

        :type: str
        """

        self._schema = ''
        """
        The schema name.

        :type: str
        """

    # ------------------------------------------------------------------------------------------------------------------
    def connect(self):
        """
        Connects to the PostgreSQL instance.
        """
        MetadataDataLayer.io = self._io
        PgSqlMetadataDataLayer.connect(self._host,
                                       self._database,
                                       self._schema,
                                       self._user,
                                       self._password,
                                       self._port)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def disconnect():
        """
        Disconnects from the PostgreSQL instance.
        """
        PgSqlMetadataDataLayer.disconnect()

    # ------------------------------------------------------------------------------------------------------------------
    def _read_configuration_file(self, filename):
        """
        Reads connections parameters from the configuration file.

        :param str filename: The path to the configuration file.
        """
        config, config_supplement = self._read_configuration(filename)

        self._host = self._get_option(config, config_supplement, 'database', 'host', fallback='localhost')
        self._user = self._get_option(config, config_supplement, 'database', 'user')
        self._password = self._get_option(config, config_supplement, 'database', 'password')
        self._database = self._get_option(config, config_supplement, 'database', 'database')
        self._schema = self._get_option(config, config_supplement, 'database', 'schema')
        self._port = int(self._get_option(config, config_supplement, 'database', 'port', fallback='5432'))

# ----------------------------------------------------------------------------------------------------------------------
