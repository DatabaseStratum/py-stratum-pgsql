"""
PyStratum

Copyright 2015-2016 Set Based IT Consultancy

Licence MIT
"""
import unittest

from cleo import CommandTester
from pystratum.application.PyStratumApplication import PyStratumApplication

from test.DataLayer import DataLayer


class AAATest(unittest.TestCase):
    """
    This test must run before all other tests because this test loads the stored routines.
    """

    # ------------------------------------------------------------------------------------------------------------------
    def test0(self):
        """
        Create tables.
        """
        DataLayer.connect('localhost', 'test', 'test', 'test', 'test', 5432)
        with open('test/ddl/create_tables.sql') as file:
            sql = file.read()

        DataLayer.execute_none(sql)

    # ------------------------------------------------------------------------------------------------------------------
    def test1(self):
        """
        Stored routine with designation type function executes a stored function and return result.
        """
        application = PyStratumApplication()
        command = application.find('stratum')
        command_tester = CommandTester(command)
        status = command_tester.execute([('command', command.get_name()),
                                         ('config_file', 'test/etc/stratum.cfg')])

        self.assertEqual(0, status)

# ----------------------------------------------------------------------------------------------------------------------
