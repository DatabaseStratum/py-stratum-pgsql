"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
from pystratum_pgsql.wrapper.PgSqlFunctionsWrapper import PgSqlFunctionsWrapper
from pystratum_pgsql.wrapper.PgSqlLogWrapper import PgSqlLogWrapper
from pystratum_pgsql.wrapper.PgSqlNoneWrapper import PgSqlNoneWrapper
from pystratum_pgsql.wrapper.PgSqlRow0Wrapper import PgSqlRow0Wrapper
from pystratum_pgsql.wrapper.PgSqlRow1Wrapper import PgSqlRow1Wrapper
from pystratum_pgsql.wrapper.PgSqlRowsWithIndexWrapper import PgSqlRowsWithIndexWrapper
from pystratum_pgsql.wrapper.PgSqlRowsWithKeyWrapper import PgSqlRowsWithKeyWrapper
from pystratum_pgsql.wrapper.PgSqlRowsWrapper import PgSqlRowsWrapper
from pystratum_pgsql.wrapper.PgSqlSingleton0Wrapper import PgSqlSingleton0Wrapper
from pystratum_pgsql.wrapper.PgSqlSingleton1Wrapper import PgSqlSingleton1Wrapper
from pystratum_pgsql.wrapper.PgSqlTableWrapper import PgSqlTableWrapper


def create_routine_wrapper(routine, lob_as_string_flag):
    """
    A factory for creating the appropriate object for generating a wrapper method for a stored routine.

    :param dict[str,str] routine: The metadata of the sored routine.
    :param str lob_as_string_flag: If True BLOBs and CLOBs must be treated as strings.

    :rtype: pystratum_pgsql.wrapper.PgSqlWrapper.PgSqlWrapper
    """
    if routine['designation'] == 'none':
        wrapper = PgSqlNoneWrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'row0':
        wrapper = PgSqlRow0Wrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'row1':
        wrapper = PgSqlRow1Wrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'rows':
        wrapper = PgSqlRowsWrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'rows_with_index':
        wrapper = PgSqlRowsWithIndexWrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'rows_with_key':
        wrapper = PgSqlRowsWithKeyWrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'singleton0':
        wrapper = PgSqlSingleton0Wrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'singleton1':
        wrapper = PgSqlSingleton1Wrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'function':
        wrapper = PgSqlFunctionsWrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'log':
        wrapper = PgSqlLogWrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'table':
        wrapper = PgSqlTableWrapper(routine, lob_as_string_flag)
    # elif routine['designation'] == 'bulk':
    #    wrapper = BulkWrapper(routine, lob_as_string_flag)
    # elif routine['designation'] == 'bulk_insert':
    #    wrapper = BulkInsertWrapper(routine, lob_as_string_flag)
    else:
        raise Exception("Unknown routine type '{0!s}'.".format(routine['designation']))

    return wrapper

# ----------------------------------------------------------------------------------------------------------------------
