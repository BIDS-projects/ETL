"""
Configuration file for database connections
"""

class MySQLConfig:
    """configuration for MySQL"""

    username = 'root'
    password = 'Cindylam1002'
    host = 'localhost'
    database = 'ecosystem_mapping'


class MongoConfig:
    """configuration for MongoDB"""

    host = 'localhost'
    port = '27017'
    database = 'dbsamples'
