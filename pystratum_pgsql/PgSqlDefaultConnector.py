from typing import Any, Dict, Union

import psycopg2

from pystratum_pgsql.PgSqlConnector import PgSqlConnector


class PgSqlDefaultConnector(PgSqlConnector):
    """
    Connects to a PostgreSQL instance using user name and password.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, params: Dict[str, Union[str, int]]):
        """
        Object constructor.
        
        :param params: The connection parameters.
        """
        self._params: Dict[str, Union[str, int]] = params

        self._connection: Any = None
        """
        The connection between Python and the PostgreSQL instance.
        """

    # ------------------------------------------------------------------------------------------------------------------
    def connect(self) -> Any:
        """
        Connects to a PostgreSQL instance.

        :rtype: psycopg2.extensions.connection
        """
        self._connection = psycopg2.connect(host=self._params['host'],
                                            user=self._params['user'],
                                            password=self._params['password'],
                                            database=self._params['database'],
                                            port=self._params['port'])
        cursor = self._connection.cursor()
        cursor.execute('set search_path to %s', (self._params['schema'],))

        return self._connection

    # ------------------------------------------------------------------------------------------------------------------
    def disconnect(self) -> None:
        """
        Disconnects from a PostgreSQL instance.
        """
        if self._connection:
            self._connection.close()
            self._connection = None

# ----------------------------------------------------------------------------------------------------------------------
