"""
PyStratum
"""
from typing import List, Dict, Union, Iterable

import psycopg2
from psycopg2.extras import RealDictCursor

from pystratum.exception.ResultException import ResultException


class StaticDataLayer:
    """
    Class for connecting to a PostgreSQL instance and executing SQL statements. Also, a parent class for classes with
    static wrapper methods for executing stored procedures and functions.
    """
    # ------------------------------------------------------------------------------------------------------------------
    connection = None
    """
    The connection between Python and the PostgreSQL instance.

    :type: psycopg2.extensions.connection
    """

    line_buffered: bool = True
    """
    If True log messages from stored procedures with designation type 'log' are line buffered (Note: In python
    sys.stdout is buffered by default).

    :type: bool
    """

    sp_log_init: str = 'stratum_log_init'
    """
    The name of the stored routine that must be run before a store routine with designation type "log".

    :type: str
    """

    sp_log_fetch: str = 'stratum_log_fetch'
    """
    The name of the stored routine that must be run after a store routine with designation type "log".

    :type: str
    """

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def _get_column_names(cursor) -> List:
        """
        Returns a list with column names retrieved from the description of a cursor.

        :param psycopg2.extensions.cursor cursor: The cursor.

        :return: list
        """
        column_names = []
        for column in cursor.description:
            column_names.append(column.name)

        return column_names

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def start_transaction(isolation_level: str = 'READ-COMMITTED', readonly: bool = False) -> None:
        """
        Starts a transaction.

        :param str isolation_level: The isolation level.
        :param bool readonly:
        """
        StaticDataLayer.connection.set_session(isolation_level, readonly)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def commit():
        """
        Commits the current transaction.
        """
        StaticDataLayer.connection.commit()

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def rollback() -> None:
        """
        Rolls back the current transaction.
        """
        StaticDataLayer.connection.rollback()

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def connect(host: str, database: str, schema: str, user: str, password: str, port: int = 5432) -> None:
        """
        Connects to a PostgreSQL instance.

        :param str host: The hostname on which the PostgreSQL server is running.
        :param str database:
        :param str schema:
        :param str user:
        :param str password:
        :param int port:
        """
        StaticDataLayer.connection = psycopg2.connect(database=database,
                                                      user=user,
                                                      password=password,
                                                      host=host,
                                                      port=port)
        cursor = StaticDataLayer.connection.cursor()
        cursor.execute('set search_path to %s', (schema,))

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def disconnect() -> None:
        """
        Disconnects from the PostgreSQL instance.
        """
        StaticDataLayer.connection.close()

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_none(sql: str, *params) -> int:
        """
        Executes a query that does not select any rows.

        :param string sql: The SQL statement.
        :param tuple params: The values for the statement.

        :rtype: int
        """
        cursor = StaticDataLayer.connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        row_count = cursor.rowcount
        cursor.close()

        return row_count

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_rows(sql: str, *params) -> List:
        """
        Executes a query that selects 0 or more rows. Returns the selected rows (an empty list if no rows are selected).

        :param str sql: The SQL statement.
        :param iterable params: The arguments for the statement.

        :rtype: list[dict]
        """
        cursor = StaticDataLayer.connection.cursor(cursor_factory=RealDictCursor)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()

        return rows

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_sp_none(sql: str, *params) -> None:
        """
        Executes a stored procedure that does not select any rows.

        Unfortunately, it is not possible to retrieve the number of affected rows of the SQL statement in the stored
        function as with execute_none (cursor.rowcount is always 1 using cursor.execute and cursor.callproc).

        :param str sql: The SQL statement.
        :param iterable params: The arguments for the statement.
        """
        cursor = StaticDataLayer.connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        cursor.close()

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_sp_row0(sql: str, *params) -> Union[None, Dict]:
        """
        Executes a stored procedure that selects 0 or 1 row. Returns the selected row or None.

        :param str sql: The SQL statement.
        :param iterable params: The arguments for the statement.

        :rtype: None|dict[str,object]
        """
        cursor = StaticDataLayer.connection.cursor()

        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        portal = StaticDataLayer.connection.cursor(cursor.fetchone()[0])
        rows = portal.fetchall()
        row_count = len(rows)
        if row_count == 1:
            column_names = StaticDataLayer._get_column_names(portal)
            ret = dict(zip(column_names, rows[0]))
        else:
            ret = None
        portal.close()
        cursor.close()

        if not (row_count == 0 or row_count == 1):
            raise ResultException('0 or 1', row_count, sql)

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_sp_row1(sql: str, *params) -> Dict:
        """
        Executes a stored procedure that selects 1 row. Returns the selected row.

        :param str sql: The SQL call the the stored procedure.
        :param iterable params: The arguments for the stored procedure.

        :rtype: dict[str,object]
        """
        cursor = StaticDataLayer.connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        portal = StaticDataLayer.connection.cursor(cursor.fetchone()[0])
        rows = portal.fetchall()
        StaticDataLayer._get_column_names(portal)
        row_count = len(rows)
        if row_count == 1:
            column_names = StaticDataLayer._get_column_names(portal)
            ret = dict(zip(column_names, rows[0]))
        else:
            ret = None  # Keep our IDE happy.
        portal.close()
        cursor.close()

        if row_count != 1:
            raise ResultException('1', row_count, sql)

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_sp_rows(sql: str, *params) -> List:
        """
        Executes a stored procedure that selects 0 or more rows. Returns the selected rows (an empty list if no rows
        are selected).

        :param str sql: The SQL call the the stored procedure.
        :param iterable params: The arguments for the stored procedure.

        :rtype: list[dict[str,object]]
        """
        cursor = StaticDataLayer.connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        portal = StaticDataLayer.connection.cursor(cursor.fetchone()[0])
        tmp = portal.fetchall()
        column_names = StaticDataLayer._get_column_names(portal)
        portal.close()
        cursor.close()

        ret = []
        for row in tmp:
            ret.append(dict(zip(column_names, row)))

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_sp_singleton0(sql: str, *params) -> object:
        """
        Executes a stored procedure that selects 0 or 1 row with 1 column. Returns the value of selected column or None.

        :param str sql: The SQL call the the stored procedure.
        :param iterable params: The arguments for the stored procedure.

        :rtype: object
        """
        cursor = StaticDataLayer.connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        portal = StaticDataLayer.connection.cursor(cursor.fetchone()[0])
        rows = portal.fetchall()
        row_count = len(rows)
        if row_count == 1:
            ret = rows[0][0]
        else:
            ret = None  # Keep our IDE happy.
        portal.close()
        cursor.close()

        if not (row_count == 0 or row_count == 1):
            raise ResultException('0 or 1', row_count, sql)

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_singleton1(sql: str, *params) -> object:
        """
        Executes query that selects 1 row with 1 column. Returns the value of the selected column.

        :param str sql: The SQL call the the stored procedure.
        :param iterable params: The arguments for the stored procedure.

        :rtype: object
        """
        cursor = StaticDataLayer.connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        row_count = cursor.rowcount
        if row_count == 1:
            ret = cursor.fetchone()[0]
        else:
            ret = None  # Keep our IDE happy.
        cursor.close()

        if row_count != 1:
            raise ResultException('1', row_count, sql)

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_sp_singleton1(sql: str, *params) -> object:
        """
        Executes a stored procedure that selects 1 row with 1 column. Returns the value of the selected column.

        :param str sql: The SQL call the the stored procedure.
        :param iterable params: The arguments for the stored procedure.

        :rtype: object
        """
        cursor = StaticDataLayer.connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        portal = StaticDataLayer.connection.cursor(cursor.fetchone()[0])
        rows = portal.fetchall()
        row_count = len(rows)
        if row_count == 1:
            ret = rows[0][0]
        else:
            ret = None  # Keep our IDE happy.
        portal.close()
        cursor.close()

        if len(rows) != 1:
            raise ResultException('1', row_count, sql)

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_sp_table(sql: str, *params) -> int:
        """
        Executes a stored routine with designation type "table". Returns the number of rows.

        :param str sql: The SQL calling the the stored procedure.
        :param iterable params: The arguments for calling the stored routine.

        :rtype: int
        """
        # todo methods for showing table
        raise NotImplementedError()

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_sp_log(sql: str, *params) -> int:
        """
        Executes a stored procedure with log statements. Returns the number of lines in the log.

        :param str sql: The SQL call the the stored procedure.
        :param iterable params: The arguments for the stored procedure.

        :rtype: int
        """
        cursor = StaticDataLayer.connection.cursor()

        # Create temporary table for logging.
        cursor.callproc(StaticDataLayer.sp_log_init)

        # Execute the stored procedure.
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        # Fetch the log messages.
        cursor.callproc(StaticDataLayer.sp_log_fetch)
        portal = StaticDataLayer.connection.cursor(cursor.fetchone()[0])
        messages = portal.fetchall()
        portal.close()
        cursor.close()

        # Log the log messages.
        for message in messages:
            print('{0!s} {1!s}'.format(*message), flush=StaticDataLayer.line_buffered)

        return len(messages)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def copy_from(file: object,
                  table: str,
                  sep: str = '\t',
                  null: str = '\\N',
                  size: int = 8192,
                  columns: Union[None, Iterable] = None) -> int:
        """
        Read data from the file-like object file appending them to the table named table. Returns the number of rows
        copied.

        :param T file: File-like object to read data from. It must have both read() and readline() methods.
        :param str table: Name of the table to copy data into.
        :param str sep: Columns separator expected in the file. Defaults to a tab.
        :param str null: Textual representation of NULL in the file. The default is the two characters string \\N.
        :param int size: Size of the buffer used to read from the file.
        :param iterable columns: Iterable with name of the columns to import. The length and types should match the
                                 content of the file to read. If not specified, it is assumed that the entire table
                                 matches the file structure.

        :rtype: int
        """
        cursor = StaticDataLayer.connection.cursor()
        cursor.copy_from(file, table, sep, null, size, columns)
        row_count = cursor.rowcount
        cursor.close()

        return row_count

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def copy_to(file: object,
                table: str,
                sep: str = '\t',
                null: str = '\\N',
                columns: Union[None, Iterable] = None) -> int:
        """
        Write the content of the table named table to the file-like object file. Returns the number of rows copied.

        :param T file: File-like object to write data into. It must have a write() method.
        :param str table: Name of the table to copy data from.
        :param str sep: Columns separator expected in the file. Defaults to a tab.
        :param str null: Textual representation of NULL in the file. The default is the two characters string \\N.
        :param iterable columns: Iterable with name of the columns to export. If not specified, export all the columns.

        :rtype: int
        """
        cursor = StaticDataLayer.connection.cursor()
        cursor.copy_to(file, table, sep, null, columns)
        row_count = cursor.rowcount
        cursor.close()

        return row_count

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def copy_expert(sql: str, file: object, size: int = 8192) -> int:
        """
        Submit a user-composed COPY statement. Returns the number of rows copied.

        :param str sql: The COPY statement to execute.
        :param T file: A file-like object to read or write (according to sql).
        :param int size: Size of the read buffer to be used in COPY FROM.

        :rtype: int
        """
        cursor = StaticDataLayer.connection.cursor()
        cursor.copy_expert(sql, file, size)
        row_count = cursor.rowcount
        cursor.close()

        return row_count

# ----------------------------------------------------------------------------------------------------------------------
