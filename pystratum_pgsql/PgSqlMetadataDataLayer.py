"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
from pystratum.MetadataDataLayer import MetadataDataLayer
from pystratum_pgsql.StaticDataLayer import StaticDataLayer


class PgSqlMetadataDataLayer(MetadataDataLayer):
    """
    Data layer for retrieving metadata and loading stored routines.
    """
    __dl = None
    """
    The connection to the PostgreSQL instance.

    :type: pystratum_pgsql.StaticDataLayer.StaticDataLayer|None
    """
    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def call_stored_routine(routine_name):
        """
        Class a stored procedure without arguments.

        :param str routine_name: The name of the procedure.

        :rtype: int|None
        """
        sql = 'call {0}()'.format(routine_name)

        return PgSqlMetadataDataLayer.execute_none(sql)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def check_table_exists(table_name):
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

        return PgSqlMetadataDataLayer.execute_none(sql)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def commit():
        """
        Commits the current transaction.
        """
        PgSqlMetadataDataLayer.__dl.commit()

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def connect(host, database, schema, user, password, port=5432):
        """
        Connects to a PostgreSQL instance.

        :param str host: The hostname on which the PostgreSQL server is running.
        :param str database:
        :param str schema:
        :param str user:
        :param str password:
        :param int port:
        """
        PgSqlMetadataDataLayer.__dl = StaticDataLayer()
        PgSqlMetadataDataLayer.__dl.connect(host, database, schema, user, password, port)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def describe_table(table_name):
        """
        Describes a table.

        :param str table_name: The name of the table.

        :rtype: list[dict[str,Object]]
        """
        sql = 'describe `{0}`'.format(table_name)

        return PgSqlMetadataDataLayer.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def disconnect():
        """
        Disconnects from the MySQL instance.
        """
        PgSqlMetadataDataLayer.__dl.disconnect()

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def drop_stored_routine(routine_type, routine_name, routine_args):
        """
        Drops a stored routine if it exists.

        :param str routine_type: The type of the routine (i.e. PROCEDURE or FUNCTION).
        :param str routine_name: The name of the routine.
        :param str routine_args: The routine arguments types as comma separated list.
        """
        sql = 'drop {0} if exists {1}({2})'.format(routine_type, routine_name, routine_args)

        PgSqlMetadataDataLayer.execute_none(sql)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def drop_temporary_table(table_name):
        """
        Drops a temporary table.

        :param str table_name: The name of the table.
        """
        sql = 'drop temporary table `{0}`'.format(table_name)

        PgSqlMetadataDataLayer.execute_none(sql)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_none(query):
        """
        Executes a query that does not select any rows.

        :param str query: The query.

        :rtype: int
        """
        PgSqlMetadataDataLayer._log_query(query)

        return PgSqlMetadataDataLayer.__dl.execute_none(query)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_rows(query):
        """
        Executes a query that selects 0 or more rows. Returns the selected rows (an empty list if no rows are selected).

        :param str query: The query.

        :rtype: list[dict[str,Object]]
        """
        PgSqlMetadataDataLayer._log_query(query)

        return PgSqlMetadataDataLayer.__dl.execute_rows(query)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def execute_singleton1(query):
        """
        Executes SQL statement that selects 1 row with 1 column. Returns the value of the selected column.

        :param str query: The query.

        :rtype: Object
        """
        PgSqlMetadataDataLayer._log_query(query)

        return PgSqlMetadataDataLayer.__dl.execute_singleton1(query)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_all_table_columns():
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

        return PgSqlMetadataDataLayer.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_label_tables(regex):
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

        return PgSqlMetadataDataLayer.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_labels_from_table(table_name, id_column_name, label_column_name):
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

        return PgSqlMetadataDataLayer.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_routine_parameters(routine_name):
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

        return PgSqlMetadataDataLayer.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_routines():
        """
        Selects metadata of all routines in the current schema.

        :rtype: list[dict[str,Object]]
        """
        sql = """
select t1.routine_name                                                                        routine_name
,      t1.routine_type                                                                        routine_type
,      array_to_string(array_agg(case when (parameter_name is not null) then
                                   concat(t2.parameter_mode, ' ',
                                          t2.parameter_name, ' ',
                                          t2.udt_name)
                                 end order by t2.ordinal_position asc), ',')                  routine_args
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

        return PgSqlMetadataDataLayer.execute_rows(sql)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def rollback():
        """
        Rollbacks the current transaction.
        """
        PgSqlMetadataDataLayer.__dl.rollback()

# ----------------------------------------------------------------------------------------------------------------------
