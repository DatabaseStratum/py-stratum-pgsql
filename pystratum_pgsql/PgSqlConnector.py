import abc
from typing import Any


class PgSqlConnector:
    """
    Interface for classes for connecting to a PostgreSQL instances.
    """

    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def connect(self) -> Any:
        """
        Connects to a PostgreSQL instance.

        :rtype: psycopg2.extensions.connection
        """
        raise NotImplementedError()

    # ------------------------------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def disconnect(self) -> None:
        """
        Disconnects from a PostgreSQL instance.
        """
        raise NotImplementedError()

# ----------------------------------------------------------------------------------------------------------------------
