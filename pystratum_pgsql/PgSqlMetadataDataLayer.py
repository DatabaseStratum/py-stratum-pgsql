from typing import List, Union

from pystratum_backend.StratumStyle import StratumStyle
from pystratum_common.MetadataDataLayer import MetadataDataLayer

from pystratum_pgsql.PgSqlConnector import PgSqlConnector
from pystratum_pgsql.PgSqlDataLayer import PgSqlDataLayer


class PgSqlMetadataDataLayer(MetadataDataLayer):
    """
    Data layer for retrieving metadata and loading stored routines.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, io: StratumStyle, connector: PgSqlConnector):
        """
        Object constructor.

        :param PyStratumStyle io: The output decorator.
        """
        super().__init__(io)

        self.__dl: PgSqlDataLayer = PgSqlDataLayer(connector)
        """
        The connection to a PostgreSQL instance.
        """

    # ------------------------------------------------------------------------------------------------------------------
    def call_stored_routine(self, routine_name: str) -> Union[int, None]:
        """
        Class a stored procedure without arguments.

        :param str routine_name: The name of the procedure.

        :rtype: int|None
        """
        sql = 'call {0}()'.format(routine_name)

        return self.execute_none(sql)

    # ------------------------------------------------------------------------------------------------------------------
    def check_table_exists(self, table_name: str) -> Union[int, None]:
        """
        Checks if a table exists in the current schema.

        :param str table_name: The name of the table.

        :rtype: int|None
        """
        sql = """
select 1 from
information_schema.tables
where TABLE_SCHEMA = database()
and   TABLE_NAME   = '{0}'""" % table_name

        return self.execute_none(sql)

    # ------------------------------------------------------------------------------------------------------------------
    def commit(self) -> None:
        """
        Commits the current transaction.
        """
        self.__dl.commit()

    # ------------------------------------------------------------------------------------------------------------------
    def connect(self) -> None:
        """
        Connects to a PostgreSQL instance.
        """
        self.__dl.connect()

    # ------------------------------------------------------------------------------------------------------------------
    def describe_table(self, table_name: str) -> List:
        """
        Describes a table.

        :param str table_name: The name of the table.

        :rtype: list[dict[str,Object]]
        """
        sql = 'describe `{0}`'.format(table_name)

        return self.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    def disconnect(self) -> None:
        """
        Disconnects from the PostgreSQL instance.
        """
        self.__dl.disconnect()

    # ------------------------------------------------------------------------------------------------------------------
    def drop_stored_routine(self, routine_type: str, routine_name: str, routine_args: str) -> None:
        """
        Drops a stored routine if it exists.

        :param str routine_type: The type of the routine (i.e. PROCEDURE or FUNCTION).
        :param str routine_name: The name of the routine.
        :param str routine_args: The routine arguments types as comma separated list.
        """
        sql = 'drop {0} if exists {1}({2})'.format(routine_type, routine_name, routine_args)

        self.execute_none(sql)

    # ------------------------------------------------------------------------------------------------------------------
    def drop_temporary_table(self, table_name: str) -> None:
        """
        Drops a temporary table.

        :param str table_name: The name of the table.
        """
        sql = 'drop temporary table `{0}`'.format(table_name)

        self.execute_none(sql)

    # ------------------------------------------------------------------------------------------------------------------
    def execute_none(self, query: str) -> int:
        """
        Executes a query that does not select any rows.

        :param str query: The query.

        :rtype: int
        """
        self._log_query(query)

        return self.__dl.execute_none(query)

    # ------------------------------------------------------------------------------------------------------------------
    def execute_rows(self, query: str) -> List:
        """
        Executes a query that selects 0 or more rows. Returns the selected rows (an empty list if no rows are selected).

        :param str query: The query.

        :rtype: list[dict[str,Object]]
        """
        self._log_query(query)

        return self.__dl.execute_rows(query)

    # ------------------------------------------------------------------------------------------------------------------
    def execute_singleton1(self, query: str) -> object:
        """
        Executes SQL statement that selects 1 row with 1 column. Returns the value of the selected column.

        :param str query: The query.

        :rtype: Object
        """
        self._log_query(query)

        return self.__dl.execute_singleton1(query)

    # ------------------------------------------------------------------------------------------------------------------
    def get_all_table_columns(self) -> List:
        """
        Selects metadata of all columns of all tables.

        :rtype: list[dict[str,Object]]
        """
        sql = """
(
  select table_name
  ,      column_name
  ,      data_type
  ,      character_maximum_length
  ,      numeric_precision
  ,      ordinal_position
  from   information_schema.COLUMNS
  where  table_catalog = current_database()
  and    table_schema  = current_schema()
  and    table_name  similar to '[a-zA-Z0-9_]*'
  and    column_name similar to '[a-zA-Z0-9_]*'
  order by table_name
  ,        ordinal_position
)

union all

(
  select concat(table_schema,'.',table_name) table_name
  ,      column_name
  ,      data_type
  ,      character_maximum_length
  ,      numeric_precision
  ,      ordinal_position
  from   information_schema.COLUMNS
  where  1=0 and table_catalog = current_database()
  and    table_name  similar to '[a-zA-Z0-9_]*'
  and    column_name similar to '[a-zA-Z0-9_]*'
  order by table_schema
  ,        table_name
  ,        ordinal_position
)
"""

        return self.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    def get_label_tables(self, regex: str) -> List:
        """
        Selects metadata of tables with a label column.

        :param str regex: The regular expression for columns which we want to use.

        :rtype: list[dict[str,Object]]
        """
        sql = """
select t1.table_name  "table_name"
,      t1.column_name "id"
,      t2.column_name "label"
from       information_schema.columns t1
inner join information_schema.columns t2 on t1.table_name = t2.table_name
where t1.table_catalog = current_database()
and   t1.table_schema = current_schema()
and   t1.column_default like 'nextval(%%)'
and   t2.table_catalog = current_database()
and   t2.table_schema  = current_schema()
and   t2.column_name ~ '{0}'
""".format(regex)

        return self.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    def get_labels_from_table(self, table_name: str, id_column_name: str, label_column_name: str) -> List:
        """
        Selects all labels from a table with labels.

        :param str table_name: The name of the table.
        :param str id_column_name: The name of the auto increment column.
        :param str label_column_name: The name of the column with labels.

        :rtype: list[dict[str,Object]]
        """
        sql = """
select \"{0}\"  as id
,      \"{1}\"  as label
from   \"{2}\"
where   nullif(\"{1}\",'') is not null""".format(id_column_name,
                                                 label_column_name,
                                                 table_name)

        return self.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    def get_routine_parameters(self, routine_name: str) -> List:
        """
        Selects metadata of the parameters of a stored routine.

        :param str routine_name: The name of the routine.

        :rtype: list[dict[str,Object]]
        """
        sql = """
select t2.parameter_name      parameter_name
,      t2.data_type           parameter_type
,      t2.udt_name            column_type
from            information_schema.routines   t1
left outer join information_schema.parameters t2  on  t2.specific_catalog = t1.specific_catalog and
                                                      t2.specific_schema  = t1.specific_schema and
                                                      t2.specific_name    = t1.specific_name and
                                                      t2.parameter_name   is not null
where t1.routine_catalog = current_database()
and   t1.routine_schema  = current_schema()
and   t1.routine_name    = '%s'
order by t2.ordinal_position""" % routine_name

        return self.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    def get_routines(self) -> List:
        """
        Selects metadata of all routines in the current schema.

        :rtype: list[dict[str,Object]]
        """
        sql = """
select t1.routine_name                                                                        as routine_name
,      t1.routine_type                                                                        as routine_type
,      array_to_string(array_agg(case when (parameter_name is not null) then
                                   concat(t2.parameter_mode, ' ',
                                          t2.parameter_name, ' ',
                                          t2.udt_name)
                                 end order by t2.ordinal_position), ',')                      as routine_args
from            information_schema.routines   t1
left outer join information_schema.parameters t2  on  t2.specific_catalog = t1.specific_catalog and
                                                      t2.specific_schema  = t1.specific_schema and
                                                      t2.specific_name    = t1.specific_name and
                                                      t2.parameter_name   is not null
where routine_catalog = current_database()
and   routine_schema  = current_schema()
group by t1.routine_name
,        t1.routine_type
order by routine_name
"""

        return self.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    def rollback(self) -> None:
        """
        Rollbacks the current transaction.
        """
        self.__dl.rollback()

# ----------------------------------------------------------------------------------------------------------------------
