"""
PyStratum
"""
import unittest

from cleo import CommandTester
from pystratum.application.PyStratumApplication import PyStratumApplication

from pystratum_pgsql.StaticDataLayer import StaticDataLayer


class AAATest(unittest.TestCase):
    """
    This test must run before all other tests because this test loads the stored routines.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def test0(self):
        """
        Create tables.
        """
        StaticDataLayer.connect('localhost', 'test', 'test', 'test', 'test', 5432)
        with open('pystratum_test/ddl/create_tables.sql') as file:
            sql = file.read()

        StaticDataLayer.execute_none(sql)
        StaticDataLayer.commit()

    # ------------------------------------------------------------------------------------------------------------------
    def test1(self):
        """
        Stored routine with designation type function executes a stored function and return result.
        """
        application = PyStratumApplication()
        command = application.find('stratum')
        command_tester = CommandTester(command)
        status = command_tester.execute([('command', command.get_name()),
                                         ('config_file', 'pystratum_test/etc/stratum.cfg')])
        print(command_tester.get_display())

        self.assertEqual(0, status)

# ----------------------------------------------------------------------------------------------------------------------
