PyStratum-pgSQL
===============

PostgreSQL Backend for PyStratum

+-----------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------+
| Social                                                                                                                      | Release                                                                                            | Tests                                                                                                 | Code                                                                                                       |
+=============================================================================================================================+====================================================================================================+=======================================================================================================+============================================================================================================+
| .. image:: https://badges.gitter.im/SetBased/py-stratum.svg                                                                 | .. image:: https://badge.fury.io/py/PyStratum-pgSQL.svg                                            | .. image:: https://travis-ci.org/DatabaseStratum/py-stratum-pgsql.svg?branch=master                   | .. image:: https://scrutinizer-ci.com/g/DatabaseStratum/py-stratum-pgsql/badges/quality-score.png?b=master |
|   :target: https://gitter.im/SetBased/py-stratum?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge  |   :target: https://badge.fury.io/py/PyStratum-pgSQL                                                |   :target: https://travis-ci.org/DatabaseStratum/py-stratum-pgsql                                     |   :target: https://scrutinizer-ci.com/g/DatabaseStratum/py-stratum-pgsql/?branch=master                    |
|                                                                                                                             |                                                                                                    | .. image:: https://scrutinizer-ci.com/g/DatabaseStratum/py-stratum-pgsql/badges/coverage.png?b=master |                                                                                                            |
|                                                                                                                             |                                                                                                    |   :target: https://scrutinizer-ci.com/g/DatabaseStratum/py-stratum-pgsql/?branch=master               |                                                                                                            |
+-----------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------+

Overview
========
PyStratum-pgSQL is a tool and library with the following mayor functionalities:

* Loading modified and new stored routines and removing obsolete stored routines into/from a PostgreSQL instance. This PostgreSQL instance can be part of your development or a production environment.
* Enhancing the PostgreSQL stored routines with constants and custom types (based on actual table columns).
* Generating automatically a Python wrapper class for calling your stored routines. This wrapper class takes care about error handing and prevents SQL injections.
* Defining Python constants based on auto increment columns and column widths.

Documentation
=============

The documentation of PyStratum pgSQL is available at https://pystratum-pgsql.readthedocs.io/en/latest/ and the general documentation of all Stratum projects is available at https://stratum.readthedocs.io/.

License
=======

This project is licensed under the terms of the MIT license.
