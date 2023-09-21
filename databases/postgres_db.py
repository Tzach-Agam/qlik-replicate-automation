import psycopg2
from configurations.config_manager import ConfigurationManager

class PostgreSQLDatabase:
    """ A class for interacting with a PostgresSQL database.
        This class facilitates interactions with a PostgresSQL database, allowing to perform various operations such as connecting
        to the database, executing SQL queries, creating schemas and tables, dropping schemas and tables, and more."""
    def __init__(self, config_manager: ConfigurationManager, section):
        """ Initialize the PostgreSQLDatabase instance.
            :param config_manager: An instance of ConfigurationManager to retrieve database configuration.
            :param section: The name of the configuration section containing database connection details. """

        self.config = config_manager.get_section(section)
        self.connection = None
        self.cursor = None

    def connect(self):
        """ Establish a connection to the PostgresSQL database using the connection string provided during object
            initialization. It also initializes a cursor to execute SQL queries on the database. """

        try:
            self.connection = psycopg2.connect(
                host=self.config["host"],
                user=self.config["username"],
                password=self.config["password"],
                database=self.config["database"]
            )
            self.cursor = self.connection.cursor()
        except psycopg2.Error as e:
            print("Error while connecting to the PostgreSQL database:", e)

    def execute_query(self, query):
        """ Execute a SQL query on the connected database.
            This method takes a SQL query as a parameter and executes it on the connected database using the cursor. It
            also commits the transaction to persist changes.
            :param query: The SQL query to execute. """

        if not self.cursor:
            raise Exception("Connection is not established. Call 'connect' method first.")
        self.cursor.execute(query)
        self.connection.commit()  # Commit the transaction

    def fetch_results(self):
        """ Fetch the results of the last executed query.
            This method retrieves the results of the last executed SQL query using the cursor and returns them as a list
            of rows, where each row is represented as a tuple."""

        if not self.cursor:
            raise Exception("Connection is not established. Call 'connect()' method first.")
        return self.cursor.fetchall()

    def create_schema(self, schema_name):
        """ Create a new schema in the connected database.
            :param schema_name: The name of the schema to create. """

        create_schema_query = f"CREATE SCHEMA {schema_name}"
        try:
            self.execute_query(create_schema_query)
        except psycopg2.Error as e:
            print(f"Error while creating schema '{schema_name}':", e)

    def create_table(self, schema_name, table_name, columns):
        """ Create a new table in the connected database schema with the specified columns.
             :param schema_name: The name of the schema where the table will be created.
             :param table_name: The name of the table to create.
             :param columns: A list of column definitions in the format "column_name data_type". """

        columns_definition = ', '.join(columns)
        create_table_query = f"CREATE TABLE {schema_name}.{table_name} ({columns_definition})"
        self.execute_query(create_table_query)

    def drop_schema(self, schema_name):
        """ Drop an existing schema in the connected database.
            :param schema_name: The name of the schema to drop. """

        drop_schema_query = f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"
        try:
            self.execute_query(drop_schema_query)
        except psycopg2.Error as e:
            print(f"Error while dropping schema '{schema_name}':", e)

    def drop_table(self, schema_name, table_name):
        """ Drop an existing table in the connected database schema.
            :param schema_name: The name of the schema containing the table to drop.
            :param table_name: The name of the table to drop. """

        drop_table_query = f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
        self.execute_query(drop_table_query)

    def drop_all_tables_in_schema(self, schema_name):
        """ Drop all tables in the specified schema in the connected database.
            This method retrieves the names of all tables within the specified schema in the connected Postgres database
            and iteratively drops each table. It also prints a success message for each dropped table.
            :param schema_name: The name of the schema containing the tables to drop. """

        get_tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
        try:
            self.cursor.execute(get_tables_query)
            tables = self.fetch_results()
            for table in tables:
                table_name = table[0]
                drop_table_query = f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
                self.execute_query(drop_table_query)
                print(f"Table '{table_name}' in schema '{schema_name}' dropped successfully.")
        except psycopg2.Error as e:
            print(f"Error while dropping tables in schema '{schema_name}':", e)

    def close(self):
        """ Close the database connection and associated cursor."""

        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
