#!/usr/bin/python

""" Connect and interact with a SQL Server database

Contains a class used for connecting and interacting with a SQL Server database.

"""

from usanapy.Util.Logging import Logging
from usanapy.Util.OSHelpers import get_log_filepath

import pandas
import urllib
import pyodbc
import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker


class SqlDatabase:
    """Connect and interact with a SQL Server database"""

    def __init__(self, server, database, driver, port, username, password, schema_name='dbo', logging_obj=None):
        """ Create a common.DataAccess.SqlDatabase.SqlDatabase object

        Args:
            server (str): name of the SQL Server
            database (str): name of the database on the SQL Server
            driver (str): name of the driver for use in the connection string, e.g. '{ODBC Driver 13 for SQL Server}'
            port (str): SQL Server port number (typically this is 1433)
            username (str): SQL Server username (leave as '' to use Windows Authentication)
            password (str): SQL Server password (leave as '' to use Windows Authentication)
            schema_name (str): SQL Server default schema to use
            logging_obj (common.Util.Logging.Logging): initialized logging object

        Example:

            ga_db = SqlDatabase(server='ussl-sqlpat01\MSSQL2017',
                                database='GA',
                                driver='{ODBC Driver 13 for SQL Server}',
                                port='1433',
                                username='',
                                password='')

        """
        # Set class variables
        if logging_obj is None:
            log_filename = get_log_filepath('Python App')
            logging_obj = Logging(name=__name__, log_filename=log_filename, log_level_str='INFO')
        self.logging_obj = logging_obj
        self.server = server
        self.database = database
        self.driver = driver
        self.port = port
        self.username = username
        self.password = password
        self.connection_string = 'Driver=' + self.driver \
                                 + ';SERVER=' + self.server \
                                 + ',' + self.port \
                                 + ';DATABASE=' + self.database \
                                 + ';UID=' + self.username \
                                 + ';PWD=' + self.password
        self.schema_name = schema_name
        # Test connection
        self.logging_obj.log(self.logging_obj.DEBUG, "method='common.DataAccess.SqlDatabase.__init__' message='Testing connection'")
        conn = self.open_connection()
        conn.close()
        # Log initialization success
        log_msg = """
        method='common.DataAccess.SqlDatabase.__init__'
        message='Initialized a SqlDatabase object' 
        server='{server}'
        database='{database}'
        driver='{driver}'
        port='{port}'
        username='{username}'
        password='{password}'
        connection_string='{connection_string}'
        """.format(server=self.server,
                   database=self.database,
                   driver=self.driver,
                   port=self.port,
                   username=self.username,
                   password='*'*len(self.password),
                   connection_string=self.connection_string)
        self.logging_obj.log(self.logging_obj.INFO, log_msg)

    def open_connection(self):
        """ Open connection

        Opens a connection to a SQL Server database.

        Returns:
            conn: (pyodbc.Connection) connection to a SQL Server database

        """
        self.logging_obj.log(self.logging_obj.DEBUG, "method='common.DataAccess.SqlDatabase.open_connection' message='Opening SQL Server connection'")
        try:
            conn = pyodbc.connect(self.connection_string)
        except Exception as ex:
            self.logging_obj.log(self.logging_obj.ERROR,
                                 """
                                 method='common.DataAccess.SqlDatabase.open_connection'
                                 message='Error trying to open SQL Server connection'
                                 exception_message='{ex_msg}'
                                 connection_string='{cxn_str}'
                                 server='{server}'
                                 port='{port}'
                                 username='{username}'
                                 password='{password}'
                                 database='{database}'""".format(ex_msg=str(ex),
                                                                 cxn_str=self.connection_string,
                                                                 server=self.server,
                                                                 port=self.port,
                                                                 username=self.username,
                                                                 password='*'*len(self.password),
                                                                 database=self.database))
            raise ex
        else:
            self.logging_obj.log(self.logging_obj.DEBUG,
                                 """
                                 method='common.DataAccess.SqlDatabase.open_connection'
                                 message='Successfully opened SQL Server connection'
                                 connection_string='{cxn_str}'
                                 server='{server}'
                                 username='{username}'
                                 password='{password}'
                                 database='{database}'""".format(cxn_str=self.connection_string,
                                                                 server=self.server,
                                                                 username=self.username,
                                                                 password='*' * len(self.password),
                                                                 database=self.database))
        return conn

    def get_engine(self):
        """ Create a Sqlalchemy engine

        Returns:
            engine: ()

        """
        self.logging_obj.log(self.logging_obj.DEBUG, "message='Creating a sqlalchemy engine'")
        params = urllib.parse.quote_plus(self.connection_string)
        try:
            engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        except Exception as ex:
            self.logging_obj.log(self.logging_obj.ERROR,
                                 """
                                 method='common.DataAccess.SqlDatabase.get_engine'
                                 message='Error trying to create a sqlalchemy engine'
                                 exception_message='{ex_msg}'
                                 connection_string='{conn_str}'""".format(ex_msg=str(ex),
                                                                          conn_str=self.connection_string))
            raise ex
        else:
            self.logging_obj.log(self.logging_obj.DEBUG,
                                 """
                                 method='common.DataAccess.SqlDatabase.get_engine'
                                 message='Successfully created a sqlalchemy engine'
                                 connection_string='{conn_str}'
                                 """.format(conn_str=self.connection_string))
        return engine

    def get_result_set(self, query_str):
        """ Get a result set as a Pandas dataframe

        Gets a result set using the pandas.read_sql method.

        Args:
            query_str: (str) query string

        Returns:
            df: (pandas.DataFrame) result set

        """
        log_msg = """
            method='common.DataAccess.SqlDatabase.get_result_set'
            message='Getting a result set'
            query_str='{query_str}'
            """.format(query_str=query_str)
        self.logging_obj.log(self.logging_obj.INFO, log_msg)
        conn = self.open_connection()
        df = pandas.read_sql(query_str, conn)
        conn.close()
        return df

    def execute_nonquery(self, query_str):
        """ Execute a non-query

        Executes a non-query such as a CREATE TABLE or UPDATE statement.

        Args:
            query_str: (str) non-query statement

        Returns:

        """
        log_msg = """
            method='common.DataAccess.SqlDatabase.execute_nonquery'
            message='Executing a non-query'
            query_str='{query_str}'
            """.format(query_str=query_str)
        self.logging_obj.log(self.logging_obj.DEBUG, log_msg)
        conn = self.open_connection()
        curs = conn.execute(query_str)
        curs.commit()
        curs.close()
        conn.close()
        log_msg = """
                    method='common.DataAccess.SqlDatabase.execute_nonquery'
                    message='Successfully executed a non-query'
                    query_str='{query_str}'
                    """.format(query_str=query_str)
        self.logging_obj.log(self.logging_obj.DEBUG, log_msg)
        return None

    def to_staging_table(self,
                         dataframe,
                         staging_table_name,
                         insert_index=True,
                         index_label=None,
                         if_table_exists='replace',
                         bulkcopy_chunksize=1000):
        """ Puts a pandas.DataFrame into a staging table

        Puts a pandas.DataFrame into a staging table.
        This uses a bulk copy method to put data from a pandas.DataFrame into a SQL staging table.

        Args:
            dataframe: (pandas.DataFrame) dataframe with data to copy into a SQL server staging table
            staging_table_name: (str) name of the staging table to copy data into
            insert_index: (logical) indicates whether or not to insert an index
            index_label: (str) indicates the column name of the index - if None, an auto-generated index will be used
            if_table_exists: (str) indicates what pandas.DataFrame.to_sql method to use if the table already exists
            bulkcopy_chunksize: (int) number of rows to bulk copy at once

        Returns:

        """
        log_msg = """
            method='common.DataAccess.SqlDatabase.to_staging_table'
            message='Copying data into a staging table'
            staging_table_name='{staging_table_name}'
            """.format(staging_table_name=staging_table_name)
        self.logging_obj.log(self.logging_obj.INFO, log_msg)
        engine = self.get_engine()
        try:
            pandas.DataFrame.to_sql(
                self=dataframe,
                name=staging_table_name,
                con=engine,
                if_exists=if_table_exists,
                index=insert_index,
                index_label=index_label,
                chunksize=bulkcopy_chunksize)
        except Exception as ex:
            self.logging_obj.log(self.logging_obj.ERROR,
                                 """
                                 method='common.DataAccess.SqlDatabase.to_staging_table'
                                 message='Error trying to copy data into a staging table'
                                 exception_message='{ex_msg}'
                                 connection_string='{staging_table_name}'""".format(ex_msg=str(ex),
                                                                                    staging_table_name=staging_table_name))
            raise ex
        else:
            self.logging_obj.log(self.logging_obj.DEBUG,
                                 """
                                 method='common.DataAccess.SqlDatabase.to_staging_table'
                                 message='Successfully Copied data into a staging table'
                                 staging_table_name='{staging_table_name}'
                                 """.format(staging_table_name=staging_table_name))
        return None

    def truncate_table(self, table_name, schema_name=None):
        """ Truncate a table in the SQL database

        Usually used to truncate staging tables prior to populating them.

        Args:
            table_name (str): name of the table to truncate
            schema_name (str): name of the schema of the table to truncate

        Returns:

        """
        if schema_name is None:
            schema_name = self.schema_name
        query_str = "TRUNCATE TABLE {schema_name}.{table_name}".format(schema_name=schema_name, table_name=table_name)
        self.execute_nonquery(query_str)

    def get_table_class(self, table_name, engine=None):
        """ Get a table's class
        Args:
            engine:
            table_name:

        Returns:
            table_class:
        """
        if engine is None:
            engine = self.get_engine()
        Base = automap_base(metadata=sqlalchemy.MetaData(schema=self.schema_name))
        Base.prepare(engine, reflect=True)
        base_classes = Base.classes
        for index, value in enumerate(base_classes):
            class_name = value.__name__
            if class_name == table_name:
                class_index = index
        table_class = list(base_classes)[class_index]
        return table_class

    def save_dataframe_to_table(self,
                                dataframe,
                                table_name,
                                remove_id_column_before_insert=True):
        """ Save a pandas DataFrame to a table in SQL Server

        Args:
            dataframe (pandas.DataFrame): data frame of data to insert into SQL Server table
            table_name (str): name of the table

        Returns:

        """
        engine = self.get_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        table = self.get_table_class(table_name, engine)
        if remove_id_column_before_insert:
            delattr(table, table_name+"Id")  # Id columns should always be <table_name>Id (USANA standard)
            dataframe.columns = table.__table__.columns.keys()[1:]  # Id columns should always be the first column in table (for simplicity people!)
        else:
            dataframe.columns = table.__table__.columns.keys()
        dataframe = dataframe.where((pandas.notnull(dataframe)), None)  # replace NaN with None for the bulk insert

        try:
            session.bulk_insert_mappings(table, dataframe.to_dict(orient="records"), render_nulls=True)
        except IntegrityError as e:
            session.rollback()
            self.logging_obj.log(self.logging_obj.ERROR, """method='common.DataAccess.SqlDatabase.save_dataframe_to_table' 
                                                            exception_message='{ex}'""".format(ex=str(e)))
        finally:
            session.commit()
            session.close()
