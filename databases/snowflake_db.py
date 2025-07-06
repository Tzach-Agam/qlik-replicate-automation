import snowflake.connector
from configurations.config_manager import ConfigurationManager

class SnowflakeDatabase:
    """ A class for interacting with a Snowflake database on AWS.
        This class facilitates interactions with a Snowflake database, allowing to perform various operations such as
        connecting to the database, executing SQL queries, creating schemas and tables, dropping schemas and tables and more."""

    def __init__(self, config_manager: ConfigurationManager, config_section):
        """ Initialize the SnowflakeDatabase instance."""

        self.section = config_manager.get_section(config_section)
        self.connection = None
        self.cursor = None

    def connect(self):
        """ Connect to the Snowflake database."""

        try:
            self.connection = snowflake.connector.connect(user=self.section['user'], password=self.section['password'],account=self.section['account'],warehouse=self.section['warehouse'], database=self.section['database'])
            self.cursor = self.connection.cursor()
            print("Connected to Snowflake.")
        except snowflake.connector.Error as err:
            print("Error while connecting to the Snowflake database:", err)
            raise

    def disconnect(self):
        """ Disconnect from the Snowflake database."""

        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Disconnected from Snowflake.")


    def execute(self, query):
        """ Execute a SQL query."""
        if not self.cursor:
            raise Exception("Connection is not established. Call 'connect' method first.")

        self.cursor.execute(query)
        self.connection.commit()

    def fetch_results(self):
        """ Fetch the results of the last executed query.
            This method retrieves the results of the last executed SQL query using the cursor and returns them as a list
            of rows, where each row is represented as a tuple."""

        if not self.cursor:
            raise Exception("Connection is not established. Call 'connect()' method first.")
        return self.cursor.fetchall()

    def create_schema(self, schema_name):
        """ Create a new schema in the Snowflake database.
        :param schema_name: The name of the schema to create. """

        query = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";'
        self.execute(query)
        self.connection.commit()
        result_message = self.fetch_results()[0][0]
        print(result_message)

    def drop_schema(self, schema_name):
        """ Drop a schema in the Snowflake database.
        :param schema_name: The name of the schema to drop. """

        query = f'DROP SCHEMA IF EXISTS "{schema_name}";'
        self.execute(query)
        self.connection.commit()
        result_massage = self.fetch_results()[0][0]
        print(result_massage)

if __name__ == '__main__':
    config = ConfigurationManager("C:\\Users\juj\PycharmProjects\qlik-replicate-automation\configurations\config.ini")
    snowflake_db = SnowflakeDatabase(config, section='Snowflake_DB')
    snowflake_db.connect()
    snowflake_db.create_schema('selenium')
    #snowflake_db.execute(f'CREATE TABLE TEST1 ("Id Number" INT PRIMARY KEY, "First Name  " STRING(25), "  Last Name" STRING(30), "E m a i l A d d r e s s" STRING(35), "   " INTEGER) WITH CHANGE_TRACKING = TRUE;')
    snowflake_db.cursor.execute("INSERT INTO \"selenium\".TEST1 VALUES "
                                "(2, 'Alice', 'Johnson', 'alice@example.com', 1), "
                                "(3, 'Alice', 'Johnson', 'alice@example.com', 1);")

