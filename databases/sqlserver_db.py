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
        self.section_name = section
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
            print("Connected to SQL Server database successfully.")
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

    def create_table(self, schema_name:str, table_name:str, columns:list):
        """ Create a new table in the connected database schema with the specified columns.
            :param schema_name: The name of the schema where the table will be created.
            :param table_name: The name of the table to create.
            :param columns: A list of column definitions in the format "column_name data_type". """
        columns_definition = ', '.join(columns)
        create_table_query = f"CREATE TABLE {schema_name}.{table_name} ({columns_definition})"
        print("Creating table", create_table_query)
        self.execute_query(create_table_query)

    def sync_command(self, schema_name:str, sync_table:str):
        """ Inserts a row to the sync table under the schema
        :param sync_table: The name of the sync command table to create.
        :param schema_name: The name of the schema where the sync table will be created. """
        sync_query = f"INSERT INTO {schema_name}.{sync_table} DEFAULT VALUES;"
        try:
            for i in range(1):
                self.execute_query(sync_query)
        except pyodbc.Error as e:
            print(f"Error executing sync command on table '{sync_table}':", e)

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
        """
        Export schema data from SQL Server with table name, primary keys, column details, and data rows.
        Replaces NULL values with the string 'NULL' in the CSV output.
        Orders rows by the primary key(s) if they exist.
        """
        get_tables_query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}'
            ORDER BY table_name
        """
        try:
            with open(output_file, 'w', newline='', encoding="utf-8") as csvfile:
                csvwriter = csv.writer(csvfile)

                # Get all tables in the schema
                self.cursor.execute(get_tables_query)
                tables = self.fetch_results()

                for table in tables:
                    table_name = table[0]
                    if table_name.lower() == "sync_table":
                        continue

                    # 1. Write separator and table name
                    csvfile.write("--------------\n")
                    csvfile.write(f'Table: "{table_name}"\n')

                    # 2. Get primary key(s)
                    pk_query = f"""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
                          AND TABLE_NAME = '{table_name}'
                          AND TABLE_SCHEMA = '{schema_name}'
                        ORDER BY ORDINAL_POSITION
                    """
                    self.cursor.execute(pk_query)
                    pk_columns = [row[0] for row in self.fetch_results()]
                    csvfile.write(f"Primary Key: {', '.join(pk_columns) if pk_columns else 'None'}\n")

                    # 3. Get column details
                    column_query = f"""
                        SELECT column_name, data_type, character_maximum_length, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}' 
                          AND table_schema = '{schema_name}'
                        ORDER BY ordinal_position
                    """
                    self.cursor.execute(column_query)
                    columns = self.fetch_results()

                    for col_name, data_type, data_length, is_nullable in columns:
                        allow_null = "True" if is_nullable == "YES" else "False"
                        csvfile.write(
                            f"Column: {col_name}, Type: {data_type}, Length: {data_length}, AllowDBNull: {allow_null}\n")

                    csvfile.write("\n")

                    # 4. Write column headers and data
                    order_by_clause = ""
                    if pk_columns:
                        # Quote identifiers properly for SQL Server
                        order_by_clause = " ORDER BY " + ", ".join([f'[{col}]' for col in pk_columns])

                    select_query = f'SELECT * FROM [{schema_name}].[{table_name}]{order_by_clause}'
                    self.cursor.execute(select_query)
                    rows = self.fetch_results()

                    col_names = [col[0] for col in columns]
                    csvwriter.writerow(col_names)

                    # Replace None with 'NULL'
                    for row in rows:
                        csvwriter.writerow(['NULL' if val is None else val for val in row])

                    csvfile.write("\n")

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

# config = ConfigurationManager("C:\\Users\juj\PycharmProjects\qlik-replicate-automation\configurations\config.ini")
# sql_db = SQLServerDatabase(config, "MSSQL_DB")
# sql_db.connect()
# sql_db.create_table("replicate_selenium_source", "wow", )