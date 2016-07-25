from pystratum_pgsql.wrapper.FunctionsWrapper import FunctionsWrapper
from pystratum_pgsql.wrapper.LogWrapper import LogWrapper
from pystratum_pgsql.wrapper.NoneWrapper import NoneWrapper
from pystratum_pgsql.wrapper.Row0Wrapper import Row0Wrapper
from pystratum_pgsql.wrapper.Row1Wrapper import Row1Wrapper
from pystratum_pgsql.wrapper.RowsWithIndexWrapper import RowsWithIndexWrapper
from pystratum_pgsql.wrapper.RowsWithKeyWrapper import RowsWithKeyWrapper
from pystratum_pgsql.wrapper.RowsWrapper import RowsWrapper
from pystratum_pgsql.wrapper.Singleton0Wrapper import Singleton0Wrapper
from pystratum_pgsql.wrapper.Singleton1Wrapper import Singleton1Wrapper


# ----------------------------------------------------------------------------------------------------------------------
def create_routine_wrapper(routine, lob_as_string_flag):
    """
    A factory for creating the appropriate object for generating a wrapper method for a stored routine.

    :param dict[str,str] routine: The metadata of the sored routine.
    :param bool lob_as_string_flag: If True BLOBs and CLOBs must be treated as strings.

    :rtype: pystratum_pgsql.wrapper.PgSqlWrapper.PgSqlWrapper
    """
    if routine['designation'] == 'none':
        wrapper = NoneWrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'row0':
        wrapper = Row0Wrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'row1':
        wrapper = Row1Wrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'rows':
        wrapper = RowsWrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'rows_with_index':
        wrapper = RowsWithIndexWrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'rows_with_key':
        wrapper = RowsWithKeyWrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'singleton0':
        wrapper = Singleton0Wrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'singleton1':
        wrapper = Singleton1Wrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'function':
        wrapper = FunctionsWrapper(routine, lob_as_string_flag)
    elif routine['designation'] == 'log':
        wrapper = LogWrapper(routine, lob_as_string_flag)
    # elif routine['designation'] == 'table':
    #    wrapper = TableWrapper(routine, lob_as_string_flag)
    # elif routine['designation'] == 'bulk':
    #    wrapper = BulkWrapper(routine, lob_as_string_flag)
    # elif routine['designation'] == 'bulk_insert':
    #    wrapper = BulkInsertWrapper(routine, lob_as_string_flag)
    else:
        raise Exception("Unknown routine type '{0!s}'.".format(routine['designation']))

    return wrapper

# ----------------------------------------------------------------------------------------------------------------------
