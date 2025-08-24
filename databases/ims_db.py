import jpype

from configurations.config_manager import ConfigurationManager
import jaydebeapi

class IMSDatabase:
    """A class for interacting with and IMS database using the jaydebeapi.
    The class connects to an IMS database using JDBC API and IBM Developer/IMS Explorer workspace
    that is established on local machine. The class will allow options to perform various operations
    on the IMS database such as connecting to the database, executing SQL queries, inserting data, and fetching results."""

    def __init__(self, config_manager:ConfigurationManager, config_section):
        """Initialize the IMSDatabase instance."""
        self.section = config_manager.get_section(config_section)
        self.connection = None
        self.cursor = None

    def connect(self):
        """Connect to the IMS database."""
        jdbc_driver_class = self.section['jdbc_driver_class']
        jdbc_jar = self.section['jdbc_jar']
        jdbc_url = (
            f"jdbc:ims://{self.section['host']}:{self.section['port']}/xml://{self.section['psb']}:"
            f"xmlMetadataLocation={self.section['xmlMetadataLocation']};"
            "dpsbOnCommit=true;"
            "treatInvalidDecimalAsNull=true;"
            "fetchSize=0;"
            "flattenTables=true;"
        )
        conn_props = {
            "user": self.section['user'],
            "password": self.section['password']
        }
        try:
            # Suppress Java logging from IMS JDBC driver
            if not jpype.isJVMStarted():
                jpype.startJVM(classpath=[jdbc_jar])

            # Set java.util.logging level to WARNING or higher
            java_util_logging = jpype.JPackage("java.util.logging")
            logger = java_util_logging.Logger.getLogger("com.ibm.ims")
            logger.setLevel(java_util_logging.Level.WARNING)

            self.connection = jaydebeapi.connect(
                jdbc_driver_class,
                jdbc_url,
                conn_props,
                jars=jdbc_jar
            )
            print("Connected successfully to IMS!")
            self.cursor = self.connection.cursor()
        except Exception as e:
            print("Connection failed:", e)
            raise

    def execute_query(self, query):
        """Execute a SQL query on the IMS database."""
        if not self.cursor:
            raise Exception("Connection is not established. Call 'connect()' method first.")
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            print("Error executing query:", e)
            raise

    def fetch_results(self, query):
        """Fetch results from the last executed query."""
        if not self.cursor:
            raise Exception("Connection is not established. Call 'connect()' method first.")
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def sync_command(self, schema_name=None, table_name=None, column_name=None, pk_column=None, value=None):
        """Execute the SYNC command on the IMS database."""
        for i in range(1, 20):
            if schema_name is None and table_name is None:
                ims_schema = self.section['schema']
                self.execute_query(f"UPDATE \"{ims_schema}\".\"ROOT\" SET FILL_0 = 'SYNC' WHERE ROOTID = 'ROOT00000S'")
            elif schema_name is not None and table_name is None:
                self.execute_query(f"UPDATE \"{schema_name}\".\"{table_name}\" SET {column_name} = 'SYNC' WHERE {pk_column} = '{value}'")
        print("SYNC Command executed successfully")
    def close(self):
        """Close the IMS database connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            print("IMS connection closed.")
        except Exception as e:
            print("Error closing IMS connection:", e)

config = ConfigurationManager("C:\\Users\\juj\PycharmProjects\qlik-replicate-automation\configurations\config.ini")
ims_db = IMSDatabase(config, "IMS_DB")
ims_db.connect()
ims_db.cursor.execute("SELECT * FROM \"DEVPCB\".\"ALLTYPES\"")
