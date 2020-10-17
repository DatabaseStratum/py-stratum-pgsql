from typing import Any, Dict, List, Optional, Union

from pystratum_pgsql.PgSqlDataLayer import PgSqlDataLayer


class TestDataLayer(PgSqlDataLayer):
    """
    The stored routines wrappers.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def tst_constant01(self) -> Any:
        """
        Test for constant.

        :rtype: dict[str,*]
        """
        return self.execute_sp_row1("select tst_constant01()")

    # ------------------------------------------------------------------------------------------------------------------
    def tst_magic_constant01(self) -> Any:
        """
        Test for magic constant.

        :rtype: *
        """
        return self.execute_sp_singleton1("select tst_magic_constant01()")

    # ------------------------------------------------------------------------------------------------------------------
    def tst_magic_constant02(self) -> Any:
        """
        Test for magic constant.

        :rtype: *
        """
        return self.execute_sp_singleton1("select tst_magic_constant02()")

    # ------------------------------------------------------------------------------------------------------------------
    def tst_magic_constant03(self) -> Any:
        """
        Test for magic constant.

        :rtype: *
        """
        return self.execute_sp_singleton1("select tst_magic_constant03()")

    # ------------------------------------------------------------------------------------------------------------------
    def tst_magic_constant04(self) -> Any:
        """
        Test for magic constant.

        :rtype: *
        """
        return self.execute_sp_singleton1("select tst_magic_constant04()")

    # ------------------------------------------------------------------------------------------------------------------
    def tst_parameter_types01(self, p_tst_bigint: Optional[int], p_tst_int: Optional[int], p_tst_smallint: Optional[int], p_tst_bit: Optional[int], p_tst_money: Optional[float], p_tst_numeric: Union[float, int, None], p_tst_float: Union[float, int, None], p_tst_real: Optional[float], p_tst_date: Optional[str], p_tst_timestamp: Optional[str], p_tst_time6: Optional[str], p_tst_char: Optional[str], p_tst_varchar: Optional[str]) -> int:
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
        return self.execute_sp_none("select tst_parameter_types01(%s::bigint, %s::int, %s::smallint, %s::bit(4), %s::money, %s::numeric, %s::numeric, %s::real, %s::date, %s::timestamp, %s::timestamp, %s::char, %s::varchar)", p_tst_bigint, p_tst_int, p_tst_smallint, p_tst_bit, p_tst_money, p_tst_numeric, p_tst_float, p_tst_real, p_tst_date, p_tst_timestamp, p_tst_time6, p_tst_char, p_tst_varchar)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_parameter_types02(self, p_tst_bigint: Optional[int], p_tst_int: Optional[int], p_tst_smallint: Optional[int], p_tst_bit: Optional[int], p_tst_money: Optional[float], p_tst_numeric: Union[float, int, None], p_tst_float: Union[float, int, None], p_tst_real: Optional[float], p_tst_date: Optional[str], p_tst_timestamp: Optional[str], p_tst_time6: Optional[str], p_tst_char: Optional[str], p_tst_varchar: Optional[str], p_tst_text: Optional[str], p_tst_bytea: Optional[bytes]) -> int:
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
        return self.execute_sp_none("select tst_parameter_types02(%s::bigint, %s::int, %s::smallint, %s::bit(4), %s::money, %s::numeric, %s::numeric, %s::real, %s::date, %s::timestamp, %s::timestamp, %s::char, %s::varchar, %s::text, %s::bytea)", p_tst_bigint, p_tst_int, p_tst_smallint, p_tst_bit, p_tst_money, p_tst_numeric, p_tst_float, p_tst_real, p_tst_date, p_tst_timestamp, p_tst_time6, p_tst_char, p_tst_varchar, p_tst_text, p_tst_bytea)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_big_int(self, p_bigint: Optional[int]) -> Any:
        """
        Test for argument type bigint.

        :param int p_bigint: The test arguments.
                             int8

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_big_int(%s::bigint)", p_bigint)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_bit(self, p_bit: Optional[int]) -> Any:
        """
        Test for argument type bit.

        :param int p_bit: The test arguments.
                          bit

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_bit(%s::bit(4))", p_bit)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_bool(self, p_bool: Optional[bool]) -> Any:
        """
        Test for argument type boolean.

        :param bool p_bool: The test arguments.
                            bool

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_bool(%s::bool)", p_bool)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_bytea(self, p_bytea: Optional[bytes]) -> Any:
        """
        Test for argument type bytea.

        :param bytes p_bytea: The test arguments.
                              bytea

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_bytea(%s::bytea)", p_bytea)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_char(self, p_char: Optional[str]) -> Any:
        """
        Test for argument type char.

        :param str p_char: The test arguments.
                           bpchar

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_char(%s::char)", p_char)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_date(self, p_date: Optional[str]) -> Any:
        """
        Test for argument type date.

        :param str p_date: The test arguments.
                           date

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_date(%s::date)", p_date)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_int(self, p_int: Optional[int]) -> Any:
        """
        Test for argument type int.

        :param int p_int: The test arguments.
                          int4

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_int(%s::int)", p_int)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_money(self, p_money: Optional[float]) -> Any:
        """
        Test for argument type money.

        :param float p_money: The test arguments.
                              money

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_money(%s::money)", p_money)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_numeric(self, p_num: Union[float, int, None]) -> Any:
        """
        Test for argument type numeric.

        :param float|int p_num: The test arguments.
                                numeric

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_numeric(%s::numeric)", p_num)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_real(self, p_real: Optional[float]) -> Any:
        """
        Test for argument type real.

        :param float p_real: The test arguments.
                             float4

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_real(%s::real)", p_real)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_small_int(self, p_smallint: Optional[int]) -> Any:
        """
        Test for argument type small int.

        :param int p_smallint: The test arguments.
                               int2

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_small_int(%s::smallint)", p_smallint)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_text(self, p_txt: Optional[str]) -> Any:
        """
        Test for argument type text.

        :param str p_txt: 
                          text

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_text(%s::text)", p_txt)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_timestamp(self, p_ts: Optional[str]) -> Any:
        """
        Test for argument type timestamp.

        :param str p_ts: The test arguments.
                         timestamp

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_timestamp(%s::timestamp)", p_ts)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_argument_varchar(self, p_varchar: Optional[str]) -> Any:
        """
        Test for argument type varchar.

        :param str p_varchar: The test arguments.
                              varchar

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_argument_varchar(%s::varchar)", p_varchar)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_function(self, p_a: Optional[int], p_b: Optional[int]) -> Any:
        """
        Test for stored function wrapper.

        :param int p_a: Parameter A.
                        int4
        :param int p_b: Parameter B.
                        int4

        :rtype: *
        """
        return self.execute_singleton1("select tst_test_function(%s::int, %s::int)", p_a, p_b)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_log(self) -> int:
        """
        Test for designation type log.

        :rtype: int
        """
        return self.execute_sp_log("select tst_test_log()")

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_none(self, p_count: Optional[int]) -> int:
        """
        Test for designation type none.

        :param int p_count: The number of iterations.
                            int8

        :rtype: int
        """
        return self.execute_sp_none("select tst_test_none(%s::bigint)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_none_with_lob(self, p_count: Optional[int], p_blob: Optional[bytes]) -> int:
        """
        Test for designation type none with BLOB.

        :param int p_count: The number of iterations.
                            int8
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: int
        """
        return self.execute_sp_none("select tst_test_none_with_lob(%s::bigint, %s::bytea)", p_count, p_blob)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_percent_symbol(self) -> List[Dict[str, Any]]:
        """
        Test for stored function with percent symbols.

        :rtype: list[dict[str,*]]
        """
        return self.execute_sp_rows("select tst_test_percent_symbol()")

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_row0a(self, p_count: Optional[int]) -> Any:
        """
        Test for designation type row0.

        :param int p_count: The number of rows selected.
                            * 0 For a valid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4

        :rtype: None|dict[str,*]
        """
        return self.execute_sp_row0("select tst_test_row0a(%s::int)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_row0a_with_lob(self, p_count: Optional[int], p_blob: Optional[bytes]) -> Any:
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
        return self.execute_sp_row0("select tst_test_row0a_with_lob(%s::int, %s::bytea)", p_count, p_blob)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_row1a(self, p_count: Optional[int]) -> Any:
        """
        Test for designation type row1.

        :param int p_count: The number of rows selected.
                            * 0 For a invalid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4

        :rtype: dict[str,*]
        """
        return self.execute_sp_row1("select tst_test_row1a(%s::int)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_row1a_with_lob(self, p_count: Optional[int], p_blob: Optional[bytes]) -> Any:
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
        return self.execute_sp_row1("select tst_test_row1a_with_lob(%s::int, %s::bytea)", p_count, p_blob)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_rows1(self, p_count: Optional[int]) -> List[Dict[str, Any]]:
        """
        Test for designation type row1.

        :param int p_count: The number of rows selected.
                            * 0 For a invalid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4

        :rtype: list[dict[str,*]]
        """
        return self.execute_sp_rows("select tst_test_rows1(%s::int)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_rows1_with_lob(self, p_count: Optional[int], p_blob: Optional[bytes]) -> List[Dict[str, Any]]:
        """
        Test for designation type rows.

        :param int p_count: The number of rows selected.
                            int4
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: list[dict[str,*]]
        """
        return self.execute_sp_rows("select tst_test_rows1_with_lob(%s::int, %s::bytea)", p_count, p_blob)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_rows_with_index1(self, p_count: Optional[int]) -> Dict:
        """
        Test for designation type rows_with_index.

        :param int p_count: The number of rows selected.
                            int4

        :rtype: dict
        """
        ret = {}
        rows = self.execute_sp_rows("select tst_test_rows_with_index1(%s::int)", p_count)
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
    def tst_test_rows_with_index1_with_lob(self, p_count: Optional[int], p_blob: Optional[bytes]) -> Dict:
        """
        Test for designation type rows_with_index with BLOB..

        :param int p_count: The number of rows selected.
                            int4
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: dict
        """
        ret = {}
        rows = self.execute_sp_rows("select tst_test_rows_with_index1_with_lob(%s::int, %s::bytea)", p_count, p_blob)
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
    def tst_test_rows_with_key1(self, p_count: Optional[int]) -> Dict:
        """
        Test for designation type rows_with_key.

        :param int p_count: Number of rows selected.
                            int4

        :rtype: dict
        """
        ret = {}
        rows = self.execute_sp_rows("select tst_test_rows_with_key1(%s::int)", p_count)
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
    def tst_test_rows_with_key1_with_lob(self, p_count: Optional[int], p_blob: Optional[bytes]) -> Dict:
        """
        Test for designation type rows_with_key with BLOB.

        :param int p_count: The number of rows selected.
                            int4
        :param bytes p_blob: The BLOB.
                             bytea

        :rtype: dict
        """
        ret = {}
        rows = self.execute_sp_rows("select tst_test_rows_with_key1_with_lob(%s::int, %s::bytea)", p_count, p_blob)
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
    def tst_test_singleton0a(self, p_count: Optional[int]) -> Any:
        """
        Test for designation type singleton0.

        :param int p_count: The number of rows selected.
                            * 0 For a valid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4

        :rtype: *
        """
        return self.execute_sp_singleton0("select tst_test_singleton0a(%s::int)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_singleton0a_with_lob(self, p_count: Optional[int], p_blob: Optional[bytes]) -> Any:
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
        return self.execute_sp_singleton0("select tst_test_singleton0a_with_lob(%s::int, %s::bytea)", p_count, p_blob)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_singleton1a(self, p_count: Optional[int]) -> Any:
        """
        Test for designation type singleton1.

        :param int p_count: The number of rows selected.
                            * 0 For a invalid test.
                            * 1 For a valid test.
                            * 2 For a invalid test.
                            int4

        :rtype: *
        """
        return self.execute_sp_singleton1("select tst_test_singleton1a(%s::int)", p_count)

    # ------------------------------------------------------------------------------------------------------------------
    def tst_test_singleton1a_with_lob(self, p_count: Optional[int], p_blob: Optional[bytes]) -> Any:
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
        return self.execute_sp_singleton1("select tst_test_singleton1a_with_lob(%s::int, %s::bytea)", p_count, p_blob)


# ----------------------------------------------------------------------------------------------------------------------
