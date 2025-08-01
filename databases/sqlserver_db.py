import pyodbc
import csv
from configurations.config_manager import ConfigurationManager

class SQLServerDatabase:
    """ A class for interacting with a SQL Server database.
        This class facilitates interactions with a SQL Server database, allowing to perform various operations such as
        connecting to the database, executing SQL queries, creating schemas and tables, dropping schemas and tables,
        exporting data to CSV files, and more."""

    def __init__(self, config_manager: ConfigurationManager, section):
        """ Initialize the SQLServerDatabase instance.
            :param config_manager: An instance of ConfigurationManager to retrieve database configuration.
            :param section: The name of the configuration section containing database connection details. """

        self.config = config_manager.get_section(section)
        self.connection_string = f'DRIVER=SQL Server;SERVER={self.config["server"]};DATABASE={self.config["database"]};UID={self.config["username"]};PWD={self.config["password"]}'
        self.connection = None
        self.cursor = None

    def connect(self):
        """ Establish a connection to the SQL Server database using the connection string provided during object
            initialization. It also initializes a cursor to execute SQL queries on the database. """

        try:
            self.connection = pyodbc.connect(self.connection_string)
            self.cursor = self.connection.cursor()
        except pyodbc.Error as e:
            print("Error while connecting to the SQL Server database:", e)

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

    def does_schema_not_exist(self, schema):
        """ Checks if the specified schema/user does not exist in the Oracle database.
            Queries the DBA_USERS view to determine if the user exists.
            :param schema: The name of the schema/user to check for non-existence. """

        query = f"SELECT 1 FROM sys.schemas WHERE name = '{schema}'"
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return result is None

    def create_schema(self, schema_name):
        """ Create a new schema in the connected database.
            :param schema_name: The name of the schema to create. """

        if self.does_schema_not_exist(schema_name):
            create_schema_query = f"CREATE SCHEMA {schema_name}"
            self.execute_query(create_schema_query)
            print(f"Schema '{schema_name}' created.")
        else:
            print(f"Schema '{schema_name}' already exists. Skipping creation.")

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

        if self.does_schema_not_exist(schema_name):
            print(f"Schema '{schema_name}' already exists. Skipping drop.")
        else:
            drop_schema_query = f'drop schema "{schema_name}"'
            self.execute_query(drop_schema_query)
            print(f"Schema '{schema_name}' dropped")

    def drop_table(self, schema_name, table_name):
        """ Drop an existing table in the connected database schema.
            :param schema_name: The name of the schema containing the table to drop.
            :param table_name: The name of the table to drop. """

        drop_table_query = f"drop table {schema_name}.{table_name}"
        self.execute_query(drop_table_query)

    def drop_all_tables_in_schema(self, schema_name):
        """ Drop all tables in the specified schema in the connected database.
            This method retrieves the names of all tables within the specified schema in the connected SQL Server database
            and iteratively drops each table. It also prints a success message for each dropped table.
            :param schema_name: The name of the schema containing the tables to drop. """

        get_tables_query = f"SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('{schema_name}')"
        try:
            self.cursor.execute(get_tables_query)
            tables = self.fetch_results()
            for table in tables:
                drop_table_query = f"DROP TABLE {schema_name}.{table[0]}"
                self.execute_query(drop_table_query)
                print(f"Table '{table[0]}' in schema '{schema_name}' dropped successfully.")
        except pyodbc.Error as e:
            print(f"Error while dropping tables in schema '{schema_name}':", e)

    def drop_replication_constraint(self):
        """ This method drops replication constraints from the connected SQL Server database. It constructs and executes
            a dynamic SQL query to drop publications that match the pattern 'AR_PUBLICATION%'. After successfully dropping
            replication, it commits the changes to the database."""

        try:
            self.cursor.execute(
                "DECLARE @sql NVARCHAR(MAX) = N'';"
                "SELECT @sql += N'EXEC sp_droppublication @publication = ''' + name + ''';' FROM syspublications WHERE name LIKE 'AR_PUBLICATION%';"
                "EXEC sp_executesql @sql;"
            )
            self.connection.commit()
            print("Dropped replication")
        except:
            print("Failed to drop replication")

    def export_schema_data_to_csv(self, schema_name, output_file):
        """ Export data from tables in a schema to a CSV file.
            This method performs the following steps:
            1. Queries the database to retrieve the names of all tables within the specified schema.
            2. For each table, it retrieves the column names and data types.
            3. Writes the table name along with column names and their data types to the CSV file.
            4. Fetches all rows from the table and writes them to the CSV file.

            The resulting CSV file will contain data from all tables in the specified schema, with each table's data
            preceded by its name and column descriptions.

            :param schema_name: The name of the schema containing the tables to export.
            :param output_file: The name of the CSV file to write data to. """

        get_tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
        try:
            with open(output_file, 'w', newline='', encoding="utf-8") as csvfile:
                csvwriter = csv.writer(csvfile)

                self.cursor.execute(get_tables_query)
                tables = self.fetch_results()
                for table in tables:
                    table_name = table[0]

                    column_query = f"SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = '{schema_name}'"
                    self.cursor.execute(column_query)
                    columns = self.fetch_results()

                    csvwriter.writerow([f"{table_name} - {', '.join([f'{col[0]} (Data type: {col[1]}, Data length: {col[2]})' for col in columns])}"])

                    select_query = f'SELECT * FROM "{schema_name}"."{table_name}"'
                    self.cursor.execute(select_query)
                    rows = self.fetch_results()
                    csvwriter.writerow([col[0] for col in columns])
                    csvwriter.writerows(rows)
                    csvfile.write('\n')

                print(f"Data exported to '{output_file}' successfully.")
        except pyodbc.DatabaseError as e:
            print(f"Error while exporting data for schema '{schema_name}':", e)

    def close(self):
        """ Close the database connection and associated cursor.
            Closing the connection and cursor is important to ensure that database resources are freed when they are no
            longer needed. It should be called at the end of database operations to clean up resources properly. """

        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()