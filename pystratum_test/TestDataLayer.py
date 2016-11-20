from pystratum_pgsql.StaticDataLayer import StaticDataLayer


# ----------------------------------------------------------------------------------------------------------------------
class TestDataLayer(StaticDataLayer):
    """
    The stored routines wrappers.
    """

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_constant01():
        """
        Test for constant.

        :rtype: dict[str,*]
        """
        return StaticDataLayer.execute_sp_row1("select tst_constant01()")

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_magic_constant01():
        """
        Test for magic constant.

        :rtype: *
        """
        return StaticDataLayer.execute_sp_singleton1("select tst_magic_constant01()")

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_magic_constant02():
        """
        Test for magic constant.

        :rtype: *
        """
        return StaticDataLayer.execute_sp_singleton1("select tst_magic_constant02()")

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_magic_constant03():
        """
        Test for magic constant.

        :rtype: *
        """
        return StaticDataLayer.execute_sp_singleton1("select tst_magic_constant03()")

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_magic_constant04():
        """
        Test for magic constant.

        :rtype: *
        """
        return StaticDataLayer.execute_sp_singleton1("select tst_magic_constant04()")

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_parameter_types01(p_tst_bigint, p_tst_int, p_tst_smallint, p_tst_bit, p_tst_money, p_tst_numeric, p_tst_float, p_tst_real, p_tst_date, p_tst_timestamp, p_tst_time6, p_tst_char, p_tst_varchar):
        """
        Test for all possible types of parameters excluding LOB's.

        :param int p_tst_bigint: Test parameter for tst_bigint.
                                 int8
        :param int p_tst_int: Test parameter for tst_int.
                              int4
        :param int p_tst_smallint: Test parameter for tst_smallint.
                                   int2
        :param int p_tst_bit: Test parameter for tst_bit.
                              bit
        :param float p_tst_money: Test parameter for tst_money.
                                  money
        :param float|int p_tst_numeric: Test parameter for tst_numeric.
                                        numeric
        :param float|int p_tst_float: t Test parameter for tst_float.
                                      numeric
        :param float p_tst_real: Test parameter for tst_real.
                                 float4
        :param str p_tst_date: Test parameter for tst_date.
                               date
        :param str p_tst_timestamp: Test parameter for tst_timestamp.
                                    timestamp
        :param str p_tst_time6: Test parameter for tst_time6.
                                time
        :param str p_tst_char: Test parameter for tst_char.
                               bpchar
        :param str p_tst_varchar: Test parameter for tst_varchar.
                                  varchar

        :rtype: int
        """
        return StaticDataLayer.execute_sp_none("select tst_parameter_types01(%s::bigint, %s::int, %s::smallint, %s::bit(4), %s::money, %s::numeric, %s::numeric, %s::real, %s::date, %s::timestamp, %s::timestamp, %s::char, %s::varchar)", p_tst_bigint, p_tst_int, p_tst_smallint, p_tst_bit, p_tst_money, p_tst_numeric, p_tst_float, p_tst_real, p_tst_date, p_tst_timestamp, p_tst_time6, p_tst_char, p_tst_varchar)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_parameter_types02(p_tst_bigint, p_tst_int, p_tst_smallint, p_tst_bit, p_tst_money, p_tst_numeric, p_tst_float, p_tst_real, p_tst_date, p_tst_timestamp, p_tst_time6, p_tst_char, p_tst_varchar, p_tst_text, p_tst_bytea):
        """
        Test for all possible types of parameters excluding LOB's.

        :param int p_tst_bigint: Test parameter for tst_bigint.
                                 int8
        :param int p_tst_int: Test parameter for tst_int.
                              int4
        :param int p_tst_smallint: Test parameter for tst_smallint.
                                   int2
        :param int p_tst_bit: Test parameter for tst_bit.
                              bit
        :param float p_tst_money: Test parameter for tst_money.
                                  money
        :param float|int p_tst_numeric: Test parameter for tst_numeric.
                                        numeric
        :param float|int p_tst_float: t Test parameter for tst_float.
                                      numeric
        :param float p_tst_real: Test parameter for tst_real.
                                 float4
        :param str p_tst_date: Test parameter for tst_date.
                               date
        :param str p_tst_timestamp: Test parameter for tst_timestamp.
                                    timestamp
        :param str p_tst_time6: Test parameter for tst_time6.
                                time
        :param str p_tst_char: Test parameter for tst_char.
                               bpchar
        :param str p_tst_varchar: Test parameter for tst_varchar.
                                  varchar
        :param str p_tst_text: Test parameter for tst_text.
                               text
        :param bytes p_tst_bytea: Test parameter for tst_bytea.
                                  bytea

        :rtype: int
        """
        return StaticDataLayer.execute_sp_none("select tst_parameter_types02(%s::bigint, %s::int, %s::smallint, %s::bit(4), %s::money, %s::numeric, %s::numeric, %s::real, %s::date, %s::timestamp, %s::timestamp, %s::char, %s::varchar, %s::text, %s::bytea)", p_tst_bigint, p_tst_int, p_tst_smallint, p_tst_bit, p_tst_money, p_tst_numeric, p_tst_float, p_tst_real, p_tst_date, p_tst_timestamp, p_tst_time6, p_tst_char, p_tst_varchar, p_tst_text, p_tst_bytea)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_big_int(p_bigint):
        """
        Test for argument type bigint.

        :param int p_bigint: The test arguments.
                             int8

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_big_int(%s::bigint)", p_bigint)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_bit(p_bit):
        """
        Test for argument type bit.

        :param int p_bit: The test arguments.
                          bit

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_bit(%s::bit(4))", p_bit)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_bool(p_bool):
        """
        Test for argument type boolean.

        :param bool p_bool: The test arguments.
                            bool

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_bool(%s::bool)", p_bool)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_bytea(p_bytea):
        """
        Test for argument type bytea.

        :param bytes p_bytea: The test arguments.
                              bytea

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_bytea(%s::bytea)", p_bytea)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_char(p_char):
        """
        Test for argument type char.

        :param str p_char: The test arguments.
                           bpchar

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_char(%s::char)", p_char)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_date(p_date):
        """
        Test for argument type date.

        :param str p_date: The test arguments.
                           date

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_date(%s::date)", p_date)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_int(p_int):
        """
        Test for argument type int.

        :param int p_int: The test arguments.
                          int4

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_int(%s::int)", p_int)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_money(p_money):
        """
        Test for argument type money.

        :param float p_money: The test arguments.
                              money

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_money(%s::money)", p_money)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_numeric(p_num):
        """
        Test for argument type numeric.

        :param float|int p_num: The test arguments.
                                numeric

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_numeric(%s::numeric)", p_num)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_real(p_real):
        """
        Test for argument type real.

        :param float p_real: The test arguments.
                             float4

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_real(%s::real)", p_real)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_small_int(p_smallint):
        """
        Test for argument type small int.

        :param int p_smallint: The test arguments.
                               int2

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_small_int(%s::smallint)", p_smallint)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_text(p_txt):
        """
        Test for argument type text.

        :param str p_txt: 
                          text

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_text(%s::text)", p_txt)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_timestamp(p_ts):
        """
        Test for argument type timestamp.

        :param str p_ts: The test arguments.
                         timestamp

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_timestamp(%s::timestamp)", p_ts)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_argument_varchar(p_varchar):
        """
        Test for argument type varchar.

        :param str p_varchar: The test arguments.
                              varchar

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_argument_varchar(%s::varchar)", p_varchar)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_function(p_a, p_b):
        """
        Test for stored function wrapper.

        :param int p_a: Parameter A.
                        int4
        :param int p_b: Parameter B.
                        int4

        :rtype: *
        """
        return StaticDataLayer.execute_singleton1("select tst_test_function(%s::int, %s::int)", p_a, p_b)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_log():
        """
        Test for designation type log.

        :rtype: int
        """
        return StaticDataLayer.execute_sp_log("select tst_test_log()")

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_none(p_count):
        """
        Test for designation type none.

        :param int p_count: The number of iterations.
                            int8

        :rtype: int
        """
        return StaticDataLayer.execute_sp_none("select tst_test_none(%s::bigint)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_none_with_lob(p_count, p_blob):
        """
        Test for designation type none with BLOB.

        :param int p_count: The number of iterations.
                            int8
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: int
        """
        return StaticDataLayer.execute_sp_none("select tst_test_none_with_lob(%s::bigint, %s::bytea)", p_count, p_blob)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_percent_symbol():
        """
        Test for stored function with percent symbols.

        :rtype: list[dict[str,*]]
        """
        return StaticDataLayer.execute_sp_rows("select tst_test_percent_symbol()")

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_row0a(p_count):
        """
        Test for designation type row0.

        :param int p_count: The number of rows selected.
                            * 0 For a valid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4

        :rtype: None|dict[str,*]
        """
        return StaticDataLayer.execute_sp_row0("select tst_test_row0a(%s::int)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_row0a_with_lob(p_count, p_blob):
        """
        Test for designation type row0 with BLOB.

        :param int p_count: The number of rows selected.
                            * 0 For a valid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: None|dict[str,*]
        """
        return StaticDataLayer.execute_sp_row0("select tst_test_row0a_with_lob(%s::int, %s::bytea)", p_count, p_blob)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_row1a(p_count):
        """
        Test for designation type row1.

        :param int p_count: The number of rows selected.
                            * 0 For a invalid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4

        :rtype: dict[str,*]
        """
        return StaticDataLayer.execute_sp_row1("select tst_test_row1a(%s::int)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_row1a_with_lob(p_count, p_blob):
        """
        Test for designation type row1 with BLOB.

        :param int p_count: The number of rows selected.
                            * 0 For a invalid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: dict[str,*]
        """
        return StaticDataLayer.execute_sp_row1("select tst_test_row1a_with_lob(%s::int, %s::bytea)", p_count, p_blob)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_rows1(p_count):
        """
        Test for designation type row1.

        :param int p_count: The number of rows selected.
                            * 0 For a invalid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4

        :rtype: list[dict[str,*]]
        """
        return StaticDataLayer.execute_sp_rows("select tst_test_rows1(%s::int)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_rows1_with_lob(p_count, p_blob):
        """
        Test for designation type rows.

        :param int p_count: The number of rows selected.
                            int4
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: list[dict[str,*]]
        """
        return StaticDataLayer.execute_sp_rows("select tst_test_rows1_with_lob(%s::int, %s::bytea)", p_count, p_blob)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_rows_with_index1(p_count):
        """
        Test for designation type rows_with_index.

        :param int p_count: The number of rows selected.
                            int4

        :rtype: dict
        """
        ret = {}
        rows = StaticDataLayer.execute_sp_rows("select tst_test_rows_with_index1(%s::int)", p_count)
        for row in rows:
            if row['tst_c01'] in ret:
                if row['tst_c02'] in ret[row['tst_c01']]:
                    ret[row['tst_c01']][row['tst_c02']].append(row)
                else:
                    ret[row['tst_c01']][row['tst_c02']] = [row]
            else:
                ret[row['tst_c01']] = {row['tst_c02']: [row]}

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_rows_with_index1_with_lob(p_count, p_blob):
        """
        Test for designation type rows_with_index with BLOB..

        :param int p_count: The number of rows selected.
                            int4
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: dict
        """
        ret = {}
        rows = StaticDataLayer.execute_sp_rows("select tst_test_rows_with_index1_with_lob(%s::int, %s::bytea)", p_count, p_blob)
        for row in rows:
            if row['tst_c01'] in ret:
                if row['tst_c02'] in ret[row['tst_c01']]:
                    ret[row['tst_c01']][row['tst_c02']].append(row)
                else:
                    ret[row['tst_c01']][row['tst_c02']] = [row]
            else:
                ret[row['tst_c01']] = {row['tst_c02']: [row]}

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_rows_with_key1(p_count):
        """
        Test for designation type rows_with_key.

        :param int p_count: Number of rows selected.
                            int4

        :rtype: dict
        """
        ret = {}
        rows = StaticDataLayer.execute_sp_rows("select tst_test_rows_with_key1(%s::int)", p_count)
        for row in rows:
            if row['tst_c01'] in ret:
                if row['tst_c02'] in ret[row['tst_c01']]:
                    if row['tst_c03'] in ret[row['tst_c01']][row['tst_c02']]:
                        raise Exception('Duplicate key for %s.' % str((row['tst_c01'], row['tst_c02'], row['tst_c03'])))
                    else:
                        ret[row['tst_c01']][row['tst_c02']][row['tst_c03']] = row
                else:
                    ret[row['tst_c01']][row['tst_c02']] = {row['tst_c03']: row}
            else:
                ret[row['tst_c01']] = {row['tst_c02']: {row['tst_c03']: row}}

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_rows_with_key1_with_lob(p_count, p_blob):
        """
        Test for designation type rows_with_key with BLOB.

        :param int p_count: The number of rows selected.
                            int4
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: dict
        """
        ret = {}
        rows = StaticDataLayer.execute_sp_rows("select tst_test_rows_with_key1_with_lob(%s::int, %s::bytea)", p_count, p_blob)
        for row in rows:
            if row['tst_c01'] in ret:
                if row['tst_c02'] in ret[row['tst_c01']]:
                    if row['tst_c03'] in ret[row['tst_c01']][row['tst_c02']]:
                        raise Exception('Duplicate key for %s.' % str((row['tst_c01'], row['tst_c02'], row['tst_c03'])))
                    else:
                        ret[row['tst_c01']][row['tst_c02']][row['tst_c03']] = row
                else:
                    ret[row['tst_c01']][row['tst_c02']] = {row['tst_c03']: row}
            else:
                ret[row['tst_c01']] = {row['tst_c02']: {row['tst_c03']: row}}

        return ret

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_singleton0a(p_count):
        """
        Test for designation type singleton0.

        :param int p_count: The number of rows selected.
                            * 0 For a valid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4

        :rtype: *
        """
        return StaticDataLayer.execute_sp_singleton0("select tst_test_singleton0a(%s::int)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_singleton0a_with_lob(p_count, p_blob):
        """
        Test for designation type singleton0 with BLOB..

        :param int p_count: The number of rows selected.
                            * 0 For a valid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: *
        """
        return StaticDataLayer.execute_sp_singleton0("select tst_test_singleton0a_with_lob(%s::int, %s::bytea)", p_count, p_blob)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_singleton1a(p_count):
        """
        Test for designation type singleton1.

        :param int p_count: The number of rows selected.
                            * 0 For a invalid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4

        :rtype: *
        """
        return StaticDataLayer.execute_sp_singleton1("select tst_test_singleton1a(%s::int)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def tst_test_singleton1a_with_lob(p_count, p_blob):
        """
        Test for designation type singleton1 with BLOB.

        :param int p_count: The number of rows selected.
                            * 0 For a invalid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: *
        """
        return StaticDataLayer.execute_sp_singleton1("select tst_test_singleton1a_with_lob(%s::int, %s::bytea)", p_count, p_blob)


# ----------------------------------------------------------------------------------------------------------------------
