from psycopg2 import pool


class Database(object):
    """
    Definition: Create and manage connections to postgresql server
    """

    def __init__(self,
                 server=None,
                 dbname=None):
        """
        Initialize the Database object without any connection
        :param server: Dictionary with server parameters
                        server = {'name': 'my-db',
                                    'param': {
                                        'port': '5432',
                                        'host': '127.0.0.1',
                                        'user': 'username',
                                        'password': 'passwd',
                                        'minconn': 1,
                                        'maxconn': 10000,}
                                 }
        :param dbname: Database name
        """
        self.server = server
        self.dbname = dbname
        self.params = None
        self.connection_pool = None
        self.init = False

    def __repr__(self):
        if self.connection_pool is not None and self.init:
            return "<Connected Database {} on {}>".format(self.dbname,
                                                          self.server['name'])
        else:
            return "<Disconnected Database {} on {}>".format(self.dbname,
                                                             self.server['name'])

    def initialize(self):
        """
        Initialize the cloud sql database with imported parameters
        to create a connection pool of atleast some minimum number of connections
        :return: None
        """
        server = self.server['param']
        dbname = self.dbname
        connection_params = \
            {
                'port': server['port'],
                'host': server['host'],
                'dbname': dbname,
                'user': server['user'],
                'password': server['password']
            }
        min_connections = server['minconn']
        max_connections = server['maxconn']

        self.params = [min_connections,
                       max_connections,
                       connection_params]

        # this creates a connection pool
        self.connection_pool = pool.SimpleConnectionPool(min_connections,
                                                         max_connections,
                                                         **connection_params)
        self.init = True

    def get_connection(self):
        """
        Get a copnnection from the database connection pool
        :return: connection object
        """
        return self.connection_pool.getconn()

    # put connection back into the pool
    def return_connection(self,
                          connection):
        """
        Return a connection back to the connection pool after the operation is done
        :param connection: connection object
        :return: None
        """
        self.connection_pool.putconn(connection)

    # close all connections in the pool
    def close_all_connections(self):
        """
        Close all connections of a database and the pool
        :return: None
        """
        self.connection_pool.closeall()
        self.init = False
        self.connection_pool = None


# connection pool method
class DatabaseCursor(Database):
    """
    Definition: Create a cursor for read/write operations to db
     using Database object connections
    """
    def __init__(self,
                 database=None,
                 server=None,
                 dbname=None):
        """
        Initialize the empty cursor class that will be used to read/write data without connections
        :param database: Database object
        :param server: Server dictionary with params
        :param dbname: Name of the database
        """
        super(DatabaseCursor, self).__init__(server,
                                             dbname)

        # pre connection parameters
        self.server = server
        self.dbname = dbname

        # if the input database is not initialized
        if not isinstance(database, Database):
            if isinstance(server, dict) and isinstance(dbname, str):
                self.database = Database(server,
                                         dbname)
            else:
                raise RuntimeError("No parameters for unknown database")
        else:
            self.database = database

        if not self.database.init:
            self.database.initialize()

        # post connection parameters
        self.connection_pool = self.database.connection_pool
        self.connection = None
        self.cursor = None

    def __enter__(self):
        """
        Get connections to the database and create the cursor
        """
        self.connection = self.get_connection()
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self,
                 exc_type,
                 exc_val,
                 exc_tb):
        """
        Method to return connection back to the pool and handle some exceptions during that process
        :param exc_type: Exception type (e.g. TypeError, AttributeError, ValueError)
        :param exc_val: Exception value (string explaining the exception)
        :param exc_tb: Exception traceback (usually refers to where the problem originated)
        """
        if exc_val is not None:
            print(exc_type)
            print(exc_val)
            self.connection.rollback()  # if something bad happened, then do not commit
        else:
            self.cursor.close()
            self.connection.commit()
        self.return_connection(self.connection)


class Query(Database):
    """
    class to store recent queries (under construction)
    """
    def __init__(self,
                 server=None,
                 dbname=None,
                 dbcursor=None):
        super(Query, self).__init__(server,
                                    dbname)
        self.server = server
        self.dbname = dbname
        self.dbcursor = dbcursor

    def __repr__(self):
        return "<Query table object in {}>".format(self.server['name'])

    # store the last 10000 queries
    def update_query_table(self):
        pass

    # create table
    @staticmethod
    def create_query_table(self):
        pass

    # delete the table
    @staticmethod
    def delete_query_table(cls):
        pass
