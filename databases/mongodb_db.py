from pymongo import MongoClient, errors
from configurations.config_manager import ConfigurationManager

class MongoDBDatabase:
    """Class for interacting with MongoDB database."""

    def __init__(self, config_manager: ConfigurationManager, section):
        """ Initialize the MongoDBDatabase instance.
        :param config_manager: An instance of ConfigurationManager to retrieve database configuration.
        :param section: The name of the configuration section containing database connection details. """

        self.config = config_manager.get_section(section)
        self.client = None

    def _build_uri(self):
        return (
            f"mongodb://{self.config['user']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}/"
            f"?authSource={self.config['database']}"
            f"&authMechanism={self.config['auth']}"
            f"&directConnection={self.config['direct_conn']}"
        )

    def connect(self):
        """ Connect to the MongoDB client. """

        try:
            uri = self._build_uri()
            self.client = MongoClient(uri)
            print("Connection to MongoDB established")
        except errors.ConnectionFailure as e:
            print("Could not connect to the MongoDB server: ", e)

    def disconnect(self):
        """ Disconnect from the MongoDB server. """

        if self.client:
            self.client.close()
            print("MongoDB connection closed")

    def get_database(self, database_name):
        """Returns a reference to the specified database."""

        if not self.client:
            raise RuntimeError("Client not connected. Call connect() first.")
        return self.client[database_name]

    def get_collection(self, database_name, collection_name):
        """Returns a reference to the specified collection."""

        return self.get_database(database_name)[collection_name]

    def drop_database(self, database_name):
        if not self.client:
            raise RuntimeError("Client not connected. Call connect() first.")
        self.client.drop_database(database_name)
        print("Dropped database " + database_name)


if __name__ == "__main__":
    config = ConfigurationManager("C:\\Users\juj\PycharmProjects\qlik-replicate-automation\configurations\config.ini")
    mongodb_schema = config.get_section("Default_Schemas")["source_schema"]
    mongodb_table = config.get_section("Default_Tables")["default_table"]
    mongodb_db = MongoDBDatabase(config, "MongoDB_DB")
    mongodb_db.connect()
    mongodb_db.drop_database(mongodb_schema)
    # mongo_database = mongodb_db.get_database(mongodb_schema)
    # print("MongoDB database: ", mongo_database)
    # mongo_collection = mongodb_db.get_collection(mongodb_schema, mongodb_table)
    # print("MongoDB collection: ", mongo_collection)
    # mongo_collection.insert_one({
    #     "name": "Alice",
    #     "age": 30,
    #     "email": "alice@example.com"
    # })
    # result = mongo_collection.insert_many([
    # {"name": "Bob", "age": 25},
    # {"name": "Charlie", "age": 35},
    # ])
    # print(result.inserted_ids)
    # for doc in mongo_collection.find():
    #     print(doc)
    # print("Document count:", mongo_collection.count_documents({}))
    # delete = mongo_collection.delete_one({"name": "Alice"})
    # print(delete.deleted_count)







