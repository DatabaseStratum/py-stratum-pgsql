from codecs import open
from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
        name='PyStratum-pgSQL',

        version='1.0.1',

        description='PyStratum-pgSQL: PostgresSQL Backend',
        long_description=long_description,

        url='https://github.com/DatabaseStratum/py-stratum-pgsql',

        author='Set Based IT Consultancy',
        author_email='info@setbased.nl',

        license='MIT',

        classifiers=[
            'Development Status :: 5 - Production/Stable',

            'Intended Audience :: Developers',
            'Topic :: Software Development :: Build Tools',
            'Topic :: Software Development :: Code Generators',
            'Topic :: System :: Installation/Setup',

            'License :: OSI Approved :: MIT License',

            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
        ],

        keywords='PyStratum, PL/pgSQL, PostgresSQL',

        packages=find_packages(exclude=['build', 'test']),

        install_requires=['psycopg2<3, >=2.8.6',
                          'PyStratum-Backend<2, >=1.0.2',
                          'PyStratum-Common<2, >=1.0.3',
                          'PyStratum-Middle<2, >=1.0.0'],
)
