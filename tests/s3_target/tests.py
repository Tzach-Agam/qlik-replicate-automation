from unittest import TestCase
from browsers.browsers import *
from configurations.config_manager import ConfigurationManager
from databases.sqlserver_db import SQLServerDatabase
from databases.s3_storage import S3Storage
from configurations.endpoint_configuration import sqlserver_source_endpoint, s3_target_endpoint
from utilities.utility_functions import move_file_to_target_dir, compare_files, log_finder
from replicate_pages import *
from time import sleep


class S3_Storage_Tests(TestCase):
    """
        This class contains Selenium test cases for Amazon S3 Storage in the Qlik Replicate software.
        It includes methods for initializing the test environment, creating tasks and endpoints,
        and performing specific test scenarios.
    """

    def setUp(self):
        """ Set up the test environment.
            Initialize the WebDriver, load configurations from the specified INI file,
            initialize databases, and initialize web pages. """

        self.driver = chrom_driver()
        self.config_manager = \
            ConfigurationManager(r"C:\Users\JUJ\PycharmProjects\qlik_replicate_automation\configurations\config.ini")
        self.logs_location, self.results_location = self.config_manager.s3_logs_results_path()
        self.sqldb = SQLServerDatabase(self.config_manager, 'MSSQL_db')
        self.s3 = S3Storage(self.config_manager, 'S3_storage')
        self._initialize_web_pages()
        self._initialize_databases()
        self._initialize_web_driver()

    def tearDown(self):
        """ Tear down the test environment.
            Drop replication constraints and close database connections. """

        self.sqldb.drop_replication_constraint()
        self.sqldb.close()
        self.driver.close()

    def _initialize_web_pages(self):
        """ Initialize web page objects for interacting with the Qlik Replicate web application. """

        self.common_functions = CommonFunctions(self.driver)
        self.tasks_general_page = TasksPage(self.driver)
        self.new_task_page = NewTaskPage(self.driver)
        self.manage_endpoints = ManageEndpoints(self.driver)
        self.task_settings = TaskSettings(self.driver)
        self.table_selection = TableSelection(self.driver)
        self.designer_page = DesignerPage(self.driver)
        self.monitor_page = MonitorPage(self.driver)

    def _initialize_databases(self):
        """ Initialize database connections and schemas for testing.
            Connect to SQL Server and S3 storage, create and configure schemas. """

        self.sqldb.connect()
        self.source_schema, self.target_schema, self.control_schema = self.config_manager.get_default_schemas()
        self.sqldb.drop_all_tables_in_schema(self.source_schema)
        self.sqldb.drop_schema(self.source_schema)
        self.s3.delete_directory(self.target_schema)
        self.sqldb.create_schema(self.source_schema)
        self.s3.create_directory(self.target_schema)

    def _initialize_web_driver(self):
        """ Initialize the Selenium WebDriver for web automation.
             Open the web application URL, maximize the browser window,
             set an implicit wait time, secure the browser connection, and handle loader icon. """

        self.driver.get(self.config_manager.get_base_url())
        self.driver.maximize_window()
        self.driver.implicitly_wait(10)
        self.common_functions.secure_browser_connection()
        self.common_functions.loader_icon_opening_replicate()

    def task_creation(self, task_name):
        """ Create a new task with the specified name in the Qlik Replicate application, configure source and target
            endpoints, and set table selection and task settings.
            :param task_name: The name of the task to be created. """

        self.tasks_general_page.create_new_task()
        self.new_task_page.new_task_creation(f'{task_name}', 'task')
        self.common_functions.task_data_loader()
        self.designer_page.choose_source_target(sqlserver_source_endpoint, s3_target_endpoint)
        self.designer_page.enter_table_selection()
        self.table_selection.choose_source_schema(self.source_schema)
        self.designer_page.enter_task_settings()
        self.task_settings.set_task_settings_general(self.target_schema, self.control_schema)

    def endpoints_creation(self):
        """ Create SQL Server source endpoint and Amazon S3 target target for testing in the Qlik Replicate application. """

        self.tasks_general_page.enter_manage_endpoints()
        self.manage_endpoints.create_rdbms_endpoint(sqlserver_source_endpoint)
        sleep(5)
        self.manage_endpoints.close()
        sleep(5)
        self.tasks_general_page.enter_manage_endpoints()
        self.manage_endpoints.create_rdbms_endpoint(s3_target_endpoint)
        sleep(5)
        self.manage_endpoints.close()

    def test_fl_cdc(self):
        """Basic replication task of FL +CDC"""

        task_name = "SQL2S3_FL_CDC"
        self.task_creation(task_name)
        self.sqldb.execute_query(f'CREATE TABLE "{self.source_schema}".test_table (A int primary key, B varchar(20));')
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (101, 'FL')"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (202, 'FL')"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (303, 'FL')"
        )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (404, 'CDC')"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (505, 'CDC')"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (606, 'CDC')"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('3')
        self.s3.wait_for_files_in_directory(f"{self.target_schema}/{self.target_schema}.test_table", desired_file_count=3)
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.s3.combine_files_to_csv_distinct(f"{self.target_schema}/{self.target_schema}.test_table", f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_add_pk(self):
        """Replicates a table with a PK constraint"""

        task_name = "SQL2S3_AddPK"
        self.task_creation(task_name)
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".test_table (a INT, b INT);')
        self.sqldb.execute_query(
            f'ALTER TABLE "{self.source_schema}".test_table ADD CONSTRAINT kuku PRIMARY KEY (a);')
        self.sqldb.cursor.execute(
             f"INSERT INTO \"{self.source_schema}\".test_table VALUES (0, 0);")
        self.sqldb.cursor.execute(
             f"INSERT INTO \"{self.source_schema}\".test_table VALUES (1, 1);")
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (2, 2);"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (3, 3);"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (4, 4);"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('3')
        self.s3.wait_for_files_in_directory(f"{self.target_schema}/{self.target_schema}.test_table", desired_file_count=3)
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.s3.combine_files_to_csv_distinct(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_pk_four_columns(self):
        """Replicates a table with pk constraint on 4 columns combined"""

        task_name = "SQL2S3_PK_4_Columns"
        self.task_creation(task_name)
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".test_table (a INTEGER NOT NULL, b CHARACTER(50) NOT NULL, c CHARACTER(10) NOT NULL, d VARCHAR(50), e BIT NOT NULL, f DATE, g FLOAT NOT NULL);')
        self.sqldb.execute_query(
            f'ALTER TABLE "{self.source_schema}".test_table ADD CONSTRAINT PK PRIMARY KEY (a,c,e,g);')
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (1, 'Bob', 'Taylor', 1, 'True', NULL, 1);"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (2, 'Frank', 'Williams', 2, 'False', NULL, 2);"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (3, 'Ellen', 'Johnson', 3, 'True', NULL, 3);"
        )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table (4, 'Jim', 'Smith', 4, 'False', NULL, 4);"
            f"INSERT INTO \"{self.source_schema}\".test_table (5, 'Mary', 'Jones', 5, 'True', NULL, 5);"
            f"INSERT INTO \"{self.source_schema}\".test_table (6, 'Linda', 'Black', 6, 'False', NULL, 6);"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('3')
        self.s3.wait_for_files_in_directory(f"{self.target_schema}/{self.target_schema}.test_table", desired_file_count=3)
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.s3.combine_files_to_csv_distinct(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_unicode_chars(self):
        """Replication of a table with UNICODE characters """

        task_name = "SQL2S3_UNICODE_CHARS"
        self.task_creation(task_name)
        self.sqldb.execute_query(f'CREATE TABLE "{self.source_schema}".test_table (a1 INT primary key, a2  VARCHAR(50), a3 VARCHAR(50), a4 VARCHAR(50), a5 VARCHAR(50), a6 VARCHAR(50),a7 VARCHAR(50),a8 VARCHAR(50),a9 VARCHAR(50),a10 VARCHAR(50),a11 VARCHAR(50), a12 VARCHAR(50), a13 VARCHAR(50), a14 VARCHAR(50), a15 VARCHAR(50), a16 VARCHAR(50), a17 VARCHAR(50), a18 VARCHAR(50), a19 VARCHAR(50), a20 VARCHAR(50), a21 VARCHAR(50), a22 VARCHAR(50), a23 VARCHAR(50), a24 VARCHAR(50), a25 VARCHAR(50), a26 VARCHAR(50), a27 VARCHAR(50), a28 VARCHAR(50), a29 VARCHAR(50), a30 VARCHAR(50), a31 VARCHAR(50), a32 VARCHAR(50), a33 VARCHAR(50));')
        self.sqldb.cursor.execute(f"INSERT INTO \"{self.source_schema}\".test_table VALUES (0, 'צוות', '©', '®', '@', '¾', 'Ã', 'Ê', 'Ø', '÷', '×', 'ñ', '¿', '«', '»', '•', '€', '‰', 'Œ', '™', 'œ', 'Ÿ', '¬', ':', 'Ø', '֍', '௵', '႟', 'í', 'ም', 'م', 'ն', 'ş');")
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (1, 'ы', 'শান্তি', 'и', '平', '和', 'ა', 'ρ', 'તિ', 'è', 'ם', 'ð', 'á', 'ತಿ', 'й', 'ន្តិ', '화', 'î', 'ч', 'ງົ', 'и', 'മാ', 'शांतता', 'г', 'မ်', 'ص', 'न्ति', 'س', 'ନ୍ତି', 'ó', 'ਤੀ', '¼', 'ñ');")
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('1')
        self.s3.wait_for_files_in_directory(f"{self.target_schema}/{self.target_schema}.test_table", desired_file_count=3)
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.s3.combine_files_to_csv_distinct(f"{self.target_schema}/{self.target_schema}.test_table", f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_special_chars(self):
        """Replication of a table with special characters """

        task_name = "SQL2S3_SPECIAL_CHARS"
        self.task_creation(task_name)
        self.sqldb.execute_query(f'CREATE TABLE "{self.source_schema}".test_table (a1 INT primary key, a2  VARCHAR(50), a3 VARCHAR(50), a4 VARCHAR(50), a5 VARCHAR(50), a6 VARCHAR(50),a7 VARCHAR(50),a8 VARCHAR(50),a9 VARCHAR(50),a10 VARCHAR(50),a11 VARCHAR(50), a12 VARCHAR(50), a13 VARCHAR(50), a14 VARCHAR(50), a15 VARCHAR(50), a16 VARCHAR(50), a17 VARCHAR(50), a18 VARCHAR(50), a19 VARCHAR(50), a20 VARCHAR(50), a21 VARCHAR(50));')
        self.sqldb.cursor.execute(f"INSERT INTO \"{self.source_schema}\".test_table VALUES (0, '`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '-', '+', '=', , ']', '|', ':');")
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(f"INSERT INTO \"{self.source_schema}\".test_table VALUES (1, '`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '-', '+', '=', , ']', '|', ':');")
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('1')
        self.s3.wait_for_files_in_directory(f"{self.target_schema}/{self.target_schema}.test_table", desired_file_count=3)
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.s3.combine_files_to_csv_distinct(f"{self.target_schema}/{self.target_schema}.test_table", f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")