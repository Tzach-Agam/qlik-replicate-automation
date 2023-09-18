import oracledb
import csv
from configurations.config_manager import ConfigurationManager

class OracleDatabase:
    """ A class for interacting with an Oracle database.
        This class facilitates interactions with an Oracle database, allowing to perform various operations such as
        connecting to the database, executing SQL queries, creating schemas and tables, dropping schemas and tables,
        exporting data to CSV files, and more."""

    def __init__(self, config_manager: ConfigurationManager, section):
        """ Initialize the OracleDatabase instance.
            :param config_manager: An instance of ConfigurationManager to retrieve database configuration.
            :param section: The name of the configuration section containing database connection details. """

        self.config = config_manager.get_section(section)
        self.connection = None
        self.cursor = None

    def connect(self):
        """ Establish a connection to the Oracle database using the connection string provided during object
            initialization. It also initializes a cursor to execute SQL queries on the database. """

        try:
            self.connection = oracledb.connect(user=self.config["username"],
                                               password=self.config["password"], dsn=self.config["dsn"])
            self.cursor = self.connection.cursor()
        except oracledb.DatabaseError as e:
            print("Error while connecting to the Oracle database:", e)

    def execute_query(self, query):
        """ Execute a SQL query on the connected database.
            This method takes a SQL query as a parameter and executes it on the connected database using the cursor. It
            also commits the transaction to persist changes.
            :param query: The SQL query to execute. """

        if not self.cursor:
            raise Exception("Connection is not established. Call 'connect()' method first.")
        self.cursor.execute(query)
        self.connection.commit()

    def fetch_results(self):
        """ Fetch the results of the last executed query.
            This method retrieves the results of the last executed SQL query using the cursor and returns them as a list
            of rows, where each row is represented as a tuple."""

        if not self.cursor:
            raise Exception("Connection is not established. Call 'connect()' method first.")
        return self.cursor.fetchall()

    def create_user(self, user_name):
        """ Create a new user in the Oracle database, and immediately give it Admin privileges. Users act like schemas
            In an oracle database.
            :param user_name: The name of the user to create. """

        create_user_query = f'create user "{user_name}" identified by oracle'
        grant_user_query = f'GRANT DBA TO "{user_name}" WITH ADMIN OPTION'
        try:
            self.execute_query(create_user_query)
            self.execute_query(grant_user_query)
        except oracledb.DatabaseError as e:
            print(f"Error while creating user '{user_name}':", e)

    def create_table(self, user_name, table_name, columns):
        """ Create a new table in the connected database user with the specified columns.
            :param user_name: The name of the user where the table will be created.
            :param table_name: The name of the table to create.
            :param columns: A list of column definitions in the format "column_name data_type". """

        columns_definition = ', '.join(columns)
        create_table_query = f"CREATE TABLE {user_name}.{table_name} ({columns_definition})"
        self.execute_query(create_table_query)

    def drop_user(self, user_name):
        """ Drop an existing user in the connected Oracle database.
            :param user_name: The name of the schema to drop. """

        drop_user_query = f'drop user "{user_name}"'
        try:
            self.execute_query(drop_user_query)
        except oracledb.DatabaseError as e:
            print(f"Error while dropping schema '{user_name}':", e)

    def drop_table(self, user_name, table_name):
        """ Drop an existing table in the connected Oracle database user.
            :param user_name: The name of the schema containing the table to drop.
            :param table_name: The name of the table to drop. """

        drop_table_query = f'drop table "{user_name}"."{table_name}"'
        self.execute_query(drop_table_query)

    def drop_all_tables_in_schema(self, schema_name):
        """ Drop all tables in the specified schema/user in the connected database.
            This method retrieves the names of all tables within the specified schema/user in the connected Oracle
            database and iteratively drops each table. It also prints a success message for each dropped table.
            :param schema_name: The name of the schema/user containing the tables to drop. """

        get_tables_query = f"SELECT table_name FROM all_tables WHERE owner = '{schema_name}'"
        try:
            self.cursor.execute(get_tables_query)
            tables = self.fetch_results()
            for table in tables:
                drop_table_query = f'DROP TABLE "{schema_name}"."{table[0]}"'
                self.execute_query(drop_table_query)
                print(f"Table '{table[0]}' in schema '{schema_name}' dropped successfully.")
        except oracledb.DatabaseError as e:
            print(f"Error while dropping tables in schema '{schema_name}':", e)

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

        get_tables_query = f"SELECT table_name FROM all_tables WHERE owner = '{schema_name}'"
        try:
            with open(output_file, 'w', newline='', encoding="utf-8") as csvfile:
                csvwriter = csv.writer(csvfile)

                self.execute_query(get_tables_query)
                tables = self.fetch_results()
                for table in tables:
                    table_name = table[0]

                    column_query = f"SELECT column_name, data_type, data_length FROM all_tab_columns WHERE table_name = '{table_name}' AND owner = '{schema_name}'"
                    self.execute_query(column_query)
                    columns = self.fetch_results()

                    csvwriter.writerow([f"{table_name} - {', '.join([f'{col[0]} (Data type: {col[1]}, Data length: {col[2]})' for col in columns])}"])

                    select_query = f'SELECT * FROM "{schema_name}"."{table_name}"'
                    self.execute_query(select_query)
                    rows = self.fetch_results()
                    csvwriter.writerow([col[0] for col in columns])
                    csvwriter.writerows(rows)
                    csvfile.write('\n')

                print(f"Data exported to '{output_file}' successfully.")
        except oracledb.DatabaseError as e:
            print(f"Error while exporting data for schema '{schema_name}':", e)

    def close(self):
        """ Close the database connection and associated cursor.
            Closing the connection and cursor is important to ensure that database resources are freed when they are no
            longer needed. It should be called at the end of database operations to clean up resources properly. """

        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()