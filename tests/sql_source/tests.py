import unittest
from unittest import TestCase
from browsers.browsers import *
from configurations.config_manager import ConfigurationManager
from databases.sqlserver_db import SQLServerDatabase
from databases.oracle_db import OracleDatabase
from configurations.endpoint_configuration import sqlserver_source_endpoint, oracle_target_endpoint
from utilities.utility_functions import move_file_to_target_dir, compare_files, log_finder
from replicate_pages import *
from time import sleep


class SQL_Source_Tests(TestCase):
    """
    This class contains Selenium test cases for SQL Server Source in the Qlik Replicate software.
    It includes methods for initializing the test environment, creating tasks and endpoints,
    and performing specific test scenarios.
    """

    def setUp(self):
        """ Set up the test environment.
            Initialize the WebDriver, load configurations from the specified INI file,
            initialize databases, and initialize web pages. """

        self.driver = chrom_driver()
        self.config_manager = \
            ConfigurationManager(r"C:\Users\JUJ\PycharmProjects\qlik_replicate_project\configurations\config.ini")
        self.logs_location, self.results_location = self.config_manager.sql_logs_results_path()
        self.sqldb = SQLServerDatabase(self.config_manager, 'MSSQL_db')
        self.oracledb = OracleDatabase(self.config_manager, 'Oracle_db')
        self._initialize_web_pages()
        self._initialize_databases()
        self._initialize_web_driver()

    def tearDown(self):
        """ Tear down the test environment.
            Drop replication constraints and close database connections. """

        self.sqldb.drop_replication_constraint()
        self.sqldb.close()
        self.oracledb.close()
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
            Connect to SQL Server and Oracle databases, create and configure schemas. """

        self.sqldb.connect()
        self.oracledb.connect()
        self.source_schema, self.target_schema, self.control_schema = self.config_manager.get_default_schemas()
        self.sqldb.drop_all_tables_in_schema(self.source_schema)
        self.oracledb.drop_all_tables_in_schema(self.target_schema)
        self.oracledb.drop_all_tables_in_schema(self.control_schema)
        self.sqldb.drop_schema(self.source_schema)
        self.oracledb.drop_user(self.target_schema)
        self.oracledb.drop_user(self.control_schema)
        self.sqldb.create_schema(self.source_schema)
        self.oracledb.create_user(self.target_schema)
        self.oracledb.create_user(self.control_schema)

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
        self.designer_page.choose_source_target(sqlserver_source_endpoint, oracle_target_endpoint)
        self.designer_page.enter_table_selection()
        self.table_selection.choose_source_schema(self.source_schema)
        self.designer_page.enter_task_settings()
        self.task_settings.set_task_settings_general(self.target_schema, self.control_schema)

    def endpoints_creation(self):
        """ Create source and target RDBMS endpoints for SQL Server and Oracle  for testing in the Qlik Replicate
            application. """

        self.tasks_general_page.enter_manage_endpoints()
        self.manage_endpoints.create_rdbms_endpoint(sqlserver_source_endpoint)
        sleep(5)
        self.manage_endpoints.close()
        sleep(5)
        self.tasks_general_page.enter_manage_endpoints()
        self.manage_endpoints.create_rdbms_endpoint(oracle_target_endpoint)
        sleep(5)
        self.manage_endpoints.close()

    def test_fl_cdc(self):
        """FL + CDC: basic test with all DMLs"""

        task_name = "SQL2Oracle_FL_CDC"
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
            f"UPDATE \"{self.source_schema}\".test_table SET B = 'UPDATE' WHERE A = 101;"
            f"DELETE FROM \"{self.source_schema}\".test_table WHERE A = 202;"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('1')
        self.monitor_page.updates_check('1')
        self.monitor_page.delete_check('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_several_tables(self):
        """Replication of 3 tables from SQL Server database to Oracle database"""

        task_name = "SQL2Oracle_Several_Tables"
        self.task_creation(task_name)
        for i in range(1, 4):
            self.sqldb.execute_query(
                f'CREATE TABLE "{self.source_schema}".test_table{str(i)} (A int primary key, B varchar(20));')
        for i in range(1, 4):
            self.sqldb.cursor.execute(
                f"INSERT INTO \"{self.source_schema}\".test_table{str(i)} VALUES (101, 'FL')"
                f"INSERT INTO \"{self.source_schema}\".test_table{str(i)} VALUES (202, 'FL')"
                f"INSERT INTO \"{self.source_schema}\".test_table{str(i)} VALUES (303, 'FL')"
            )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('3')
        self.monitor_page.cdc_tab()
        for i in range(1, 4):
            self.sqldb.cursor.execute(
                f"INSERT INTO \"{self.source_schema}\".test_table{str(i)} VALUES (404, 'CDC')"
                f"INSERT INTO \"{self.source_schema}\".test_table{str(i)} VALUES (505, 'CDC')"
                f"INSERT INTO \"{self.source_schema}\".test_table{str(i)} VALUES (606, 'CDC')"
            )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('3', '3', '3')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_char_columns(self):
        """Replication of a table with 'chars' datatype from SQL Server source to Oracle target"""

        task_name = "SQL2Oracle_Chars_Datatype"
        self.task_creation(task_name)
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".test_table (A int primary key, char1 char(36), varchar1 varchar(36), nchar1 nchar(36), nvarchar1 nvarchar(36));')
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (1, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (2, '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (3, 'a', 'a', 'a', 'a');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (4, '', '', '', '');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (5, ' ', ' ', ' ', ' ');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (6, ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (7, 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (8, null,null,null,null);"
        )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (9, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (10, '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (11, 'a', 'a', 'a', 'a');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (12, '', '', '', '');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (13, ' ', ' ', ' ', ' ');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (14, ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (15, 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ');"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (16, null,null,null,null);"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('8')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_number_columns(self):
        """Replication of a table with 'integer' datatype columns from SQL Server source to Oracle target"""

        task_name = "SQL2Oracle_Number_Datatype"
        self.task_creation(task_name)
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".test_table (a int primary key, decimal_1 DECIMAL(18,0), decimal_2 DECIMAL(9,5), decimal_3 DECIMAL(38,0), decimal_4 DECIMAL(38,38), int_1 INT, money_1 MONEY, smallint_1 SMALLINT, numeric_1 NUMERIC (5), numeric_2 NUMERIC (15,5), numeric_3 NUMERIC (25,5), numeric_4 NUMERIC (30,5), smallmoney_1 SMALLMONEY, tinyint_1 TINYINT, bigint_1 BIGINT);')
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (1, 999999999999999999, 9999.99999, 99999999999999999999999999999999999999, 0.99999999999999999999999999999999999999, -2147483648, -922337203685477.5808, -32768, -99999, 1234567890.54321, 123456789012345.54321, 12345678901234567890.54321, - 214748.3648, 255, -9225808);"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (2, 1, 1.99999, 1, 0.00000000000000000000000000000000000001, 2147483647, 922337203685477.5807, 32767, 99999, 9999999999.99999, 999999999999999.99999, 99999999999999999999.99999, 214748.3647, 88, 9223372036854775807);"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (3, 123456789 , 1000.88545, 123456789, 0.123456789, 256, 461168601842738.79035, 12345, 1, 1, 1, -99999999999999999999.99999, 0.9985, 1, 1);"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (4, -999999999999999999, -9999.99999, 123456789, 0.999999999999999999, 256, 461168601842738.79035, 0, 0, 0, 0, 0, 0, 0, 0);"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (5, null, null, null, null, null, null, null, null, null, null, null, null, null, null);"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (6, -999999999999999999, -9999.99999, -99999999999999999999999999999999999999, -0.99999999999999999999999999999999999999, 2147483647, 922337203685477.5807, 32767, 99999, -9999999999.99999, -99999999999999999999.99999, -9999999999999999999999999.99999, 214748.3647, 0, -9223372036854775808);"
        )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (7, 999999999999999999, 9999.99999, 99999999999999999999999999999999999999, 0.99999999999999999999999999999999999999, -2147483648, -922337203685477.5808, -32768, -99999, 1234567890.54321, 123456789012345.54321, 12345678901234567890.54321, - 214748.3648, 255, -9225808)"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (8, 1, 1.99999, 1, 0.00000000000000000000000000000000000001, 2147483647, 922337203685477.5807, 32767, 99999, 9999999999.99999, 999999999999999.99999, 99999999999999999999.99999, 214748.3647, 88, 9223372036854775807)"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (9, 123456789 , 1000.88545, 123456789, 0.123456789, 256, 461168601842738.79035, 12345, 1, 1, 1, -99999999999999999999.99999, 0.9985, 1, 1)"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (10, -999999999999999999, -9999.99999, 123456789, 0.999999999999999999, 256, 461168601842738.79035, 0, 0, 0, 0, 0, 0, 0, 0)"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (11, null, null, null, null, null, null, null, null, null, null, null, null, null, null)"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (12, -999999999999999999, -9999.99999, -99999999999999999999999999999999999999, -0.99999999999999999999999999999999999999, 2147483647, 922337203685477.5807, 32767, 99999, -9999999999.99999, -99999999999999999999.99999, -9999999999999999999999999.99999, 214748.3647, 0, -9223372036854775808)"

            f"UPDATE \"{self.source_schema}\".test_table SET decimal_1= 1, decimal_2=1, decimal_3=1, decimal_4=0.1, int_1=1, money_1=1, smallint_1=1, numeric_1=1, numeric_2=1, numeric_3=1, numeric_4=1, smallmoney_1=1, tinyint_1=1, bigint_1=1 where a=1;"
            f"DELETE FROM \"{self.source_schema}\".test_table WHERE a = 4;"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('6')
        self.monitor_page.updates_check('1')
        self.monitor_page.delete_check('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_column_ddl(self):
        """FL + CDC task with column related DDL statements (ADD DROP, ALTER) """

        task_name = "SQL2Oracle_DDL_Columns"
        self.task_creation(task_name)
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".test_table (COL1 int primary key, COL2 varchar(20), COL3 int);')
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (101, 'FL', 101)"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (202, 'FL', 202)"
        )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (303, 'CDC', 303)"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (404, 'CDC', 404)"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (505, 'CDC', 505)"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('3')
        self.sqldb.execute_query(f'ALTER TABLE "{self.source_schema}".test_table ADD COL4 int')
        self.monitor_page.ddl_check('1')
        self.sqldb.execute_query(f'ALTER TABLE "{self.source_schema}".test_table DROP COLUMN COL3')
        self.monitor_page.ddl_check('2')
        self.sqldb.execute_query(f'ALTER TABLE "{self.source_schema}".test_table ALTER COLUMN COL2 varchar(30)')
        self.monitor_page.ddl_check('3')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_verbose_logs(self):
        """FL + CDC task with Verbose logs configured for detailed task logs"""

        task_name = "SQL2Oracle_Verbose_Logging"
        self.task_creation(task_name)
        self.designer_page.enter_task_settings()
        self.common_functions.loader_icon_opening_replicate()
        self.task_settings.task_logging()
        self.task_settings.change_component_logging('SERVER', 'SOURCE_UNLOAD', 'TARGET_LOAD', 'SOURCE_CAPTURE',
                                                    'TARGET_APPLY', logging_level='VERBOSE')
        self.task_settings.ok_button()
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".test_table (VerboseNumber int primary key, VerboseString varchar(20));')
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (101, 'FL')"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (202, 'FL')"
        )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (303, 'CDC')"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (404, 'CDC')"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (505, 'CDC')"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('3')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        log_finder(f"{self.logs_location}/reptask_{task_name}.log", "]V:", f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_transactional_mode(self):
        """FL + CDC task with transactional mode for CDC"""

        task_name = "SQL2Oracle_FL_CDC_Transactional"
        self.task_creation(task_name)
        self.designer_page.enter_task_settings()
        self.common_functions.loader_icon_opening_replicate()
        self.task_settings.transactional_mode_change()
        self.task_settings.ok_button()
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
            f"UPDATE \"{self.source_schema}\".test_table SET B = 'UPDATE' WHERE A = 101;"
            f"DELETE FROM \"{self.source_schema}\".test_table WHERE A = 202;"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('1')
        self.monitor_page.updates_check('1')
        self.monitor_page.delete_check('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        log_finder(f"{self.logs_location}/reptask_{task_name}.log", "Working in transactional apply mode",
                   f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_reload_task(self):
        """Replication task that stops and perform reload task operation """

        task_name = "SQL2Oracle_Reload_Task"
        self.task_creation(task_name)
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".test_table (COL1_Reload int primary key, COL2_Reload varchar(20));')
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
            f"UPDATE \"{self.source_schema}\".test_table SET COL2_Reload = 'UPDATE' WHERE COL1_Reload = 101;"
            f"DELETE FROM \"{self.source_schema}\".test_table WHERE COL1_Reload = 202;"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('1')
        self.monitor_page.updates_check('1')
        self.monitor_page.delete_check('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.reload_task(task_name)
        self.designer_page.start_task_wait()
        self.tasks_general_page.open_task(task_name)
        self.designer_page.enter_monitor_page()
        self.monitor_page.fl_tab()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        self.common_functions.navigate_to_main_page('tasks')
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_100_tables(self):
        """Replication of 100 tables from SQL Server source to Oracle target"""

        task_name = "SQL2Oracle_100_Tables"
        self.task_creation(task_name)
        for i in range(1, 101):
            self.sqldb.execute_query(
                f'CREATE TABLE "{self.source_schema}".test_table{str(i)} (A int primary key, B varchar(20));')
        for i in range(1, 101):
            self.sqldb.cursor.execute(
                f"INSERT INTO \"{self.source_schema}\".test_table{str(i)} VALUES (101, 'FL')"
                f"INSERT INTO \"{self.source_schema}\".test_table{str(i)} VALUES (202, 'FL')"
                f"INSERT INTO \"{self.source_schema}\".test_table{str(i)} VALUES (303, 'FL')"
            )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('100')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_100columns(self):
        """ Replication task of a table with 100 columns"""

        task_name = "SQL2Oracle_100_Columns"
        self.task_creation(task_name)
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".test_table (a int primary key, col1 char(100), col2  char(100), col3 char(100), col4 char(100), col5 char(5), col6 char(5), col7 char(5), col8 char(5), col9 char(5), col10 char(5), col11 char(5), col12 char(5), col13 char(5), col14 char(5), col15 char(5), col16 char(5), col17 char(5), col18 char(5), col19 char(5), col20 char(5), col21 char(5), col22 char(5), col23 char(5), col24 char(5), col25 char(5), col26 char(5), col27 char(5), col28 char(5), col29 char(5), col30 char(5), col31 char(5), col32 char(5), col33 char(5), col34 char(5), col35 char(5), col36 char(5), col37 char(5), col38 char(5), col39 char(5), col40 char(5), col41 char(5), col42 char(5), col43 char(5), col44 char(5), col45 char(5), col46 char(5), col47 char(5), col48 char(5), col49 char(5), col50 char(5), col51 char(5), col52 char(5), col53 char(5), col54 char(5), col55 char(5), col56 char(5), col57 char(5), col58 char(5), col59 char(5), col60 char(5), col61 char(5), col62 char(5), col63 char(5), col64 char(5), col65 char(5), col66 char(5), col67 char(5), col68 char(5), col69 char(5), col70 char(5), col71 char(5), col72 char(5), col73 char(5), col74 char(5), col75 char(5), col76 char(5), col77 char(5), col78 char(5), col79 char(5), col80 char(5), col81 char(5), col82 char(5), col83 char(5), col84 char(5), col85 char(5), col86 char(5), col87 char(5), col88 char(5), col89 char(5), col90 char(5), col91 char(5), col92 char(5), col93 char(5), col94 char(5), col95 char(5), col96 char(5), col97 char(5), col98 char(5), col99 char(5), col100 char(5));')
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table values (1,'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL','FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL', 'FL')"
        )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_case_sensitive(self):
        """Replication task of a table upper-case and lower-case columns and cahracters"""

        task_name = "SQL2Oracle_Case_Sensitive"
        self.task_creation(task_name)
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".test_table (A int primary key, english_upper VARCHAR(20) , english_lower VARCHAR(20), chinese_upper NVARCHAR(40), chinese_lower NVARCHAR(40));')
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table values (1, 'GOOD MORNING', 'good morning', N'壹貳參肆伍陸柒捌玖拾佰仟萬', N'一二三四五六七八九十百千萬')"
            f"INSERT INTO \"{self.source_schema}\".test_table values (2, 'GOOD MORNING', 'good morning', N'壹貳參肆伍陸柒捌玖拾佰仟萬', N'一二三四五六七八九十百千萬')"
            f"INSERT INTO \"{self.source_schema}\".test_table values (3, 'GOOD MORNING', 'good morning', N'壹貳參肆伍陸柒捌玖拾佰仟萬', N'一二三四五六七八九十百千萬')"
        )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table values (4, 'GOOD MORNING', 'good morning', N'壹貳參肆伍陸柒捌玖拾佰仟萬', N'一二三四五六七八九十百千萬')"
            f"INSERT INTO \"{self.source_schema}\".test_table values (5, 'GOOD MORNING', 'good morning', N'壹貳參肆伍陸柒捌玖拾佰仟萬', N'一二三四五六七八九十百千萬')"
            f"UPDATE \"{self.source_schema}\".test_table SET english_upper='ABCDEFG' , english_lower= 'abcdef' where a=3;"
            f"UPDATE \"{self.source_schema}\".test_table SET chinese_upper=N'捌玖拾佰仟萬壹貳參肆伍陸柒' , chinese_lower=N'八九十百千萬一二三四五六七' where a=3;"
            f"DELETE FROM \"{self.source_schema}\".test_table WHERE A = 2;"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('2')
        self.monitor_page.updates_check('2')
        self.monitor_page.delete_check('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_chinese_table(self):
        """Replication of a table with Chinese characters"""

        task_name = "SQL2Oracle_Chinese_Table"
        self.task_creation(task_name)
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".新 (一 int primary key, 工人工資 NVARCHAR(25), 工人全名和生日 NVARCHAR(25), 四 NVARCHAR(25), 那是雜誌嗎 NCHAR(25), 六 NCHAR(25), 那不是杂志那是字典 NCHAR(25));')
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".新 VALUES (1,N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文')"
            f"INSERT INTO \"{self.source_schema}\".新 VALUES (2,N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文')"
            f"INSERT INTO \"{self.source_schema}\".新 VALUES (3,N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文')"
        )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".新 VALUES (4,N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文')"
            f"INSERT INTO \"{self.source_schema}\".新 VALUES (5,N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文',N'汉语漢語中文')"
            f"UPDATE \"{self.source_schema}\".新 SET 工人工資='ghijkl' , 工人全名和生日= 'ghijkl' , 四= 'ghijkl', 那是雜誌嗎= 'ghijkl' , 六= 'ghijkl', 那不是杂志那是字典= 'ghijkl' WHERE 一 = 2;"
            f"UPDATE \"{self.source_schema}\".新 SET 工人工資=N'中文文本不好' , 工人全名和生日= N'中文文本不好' , 四= N'中文文本不好', 那是雜誌嗎= N'中文文本不好' , 六= N'中文文本不好', 那不是杂志那是字典= N'中文文本不好' where 一 = 3;"
            f"DELETE FROM \"{self.source_schema}\".新 WHERE 一 = 1;"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('2')
        self.monitor_page.updates_check('2')
        self.monitor_page.delete_check('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_hebrew_table(self):
        """Replication of a table with Hebrew characters"""

        task_name = "SQL2Oracle_Hebrew_Table"
        self.task_creation(task_name)
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".טבלה_חדשה (ראשון int primary key, שני NVARCHAR(25), שלישי NVARCHAR(25), רביעי NVARCHAR(25), חמישי NVARCHAR(25), ששי NVARCHAR(25), שביעי NVARCHAR(25));')
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".טבלה_חדשה values (1,N'עברית',N'עברית',N'עברית',N'עברית',N'עברית',N'עברית')"
            f"INSERT INTO \"{self.source_schema}\".טבלה_חדשה values (2,N'עברית',N'עברית',N'עברית',N'עברית',N'עברית',N'עברית')"
            f"INSERT INTO \"{self.source_schema}\".טבלה_חדשה values (3,N'עברית',N'עברית',N'עברית',N'עברית',N'עברית',N'עברית')"
        )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".טבלה_חדשה values (4,N'עברית',N'עברית',N'עברית',N'עברית',N'עברית',N'עברית')"
            f"INSERT INTO \"{self.source_schema}\".טבלה_חדשה values (5,N'עברית',N'עברית',N'עברית',N'עברית',N'עברית',N'עברית')"
            f"UPDATE \"{self.source_schema}\".טבלה_חדשה  SET שני = N'חדש' WHERE ראשון = 3;"
            f"DELETE FROM \"{self.source_schema}\".טבלה_חדשה WHERE ראשון = 1;"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('2')
        self.monitor_page.updates_check('1')
        self.monitor_page.delete_check('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_emoji_datatype(self):
        """Replication task of a table with emojis"""

        task_name = "SQL2Oracle_Emoji"
        self.task_creation(task_name)
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".test_table (A int primary key , B NCHAR(16) , C NVARCHAR(16) , D NTEXT);')
        self.sqldb.execute_query(
            f'CREATE TABLE "{self.source_schema}".test_table1 (A1 int primary key , B1 NCHAR(16) , C1 NVARCHAR(16) , D1 NTEXT);')

        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (1,	N'😀😀',		N'😀😀',		N'😀😀')"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (2,	N'💩💩',		N'💩💩',		N'💩💩')"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (3,	N'🍕🍕',	    N'🍕🍕',		N'🍕🍕')"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (4,	N'🐷🐷',		N'🐷🐷', 		N'🐷🐷')"
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (5,	N'😱😱',	 	N'😱😱',	 	N'😱😱')"

            f"INSERT INTO \"{self.source_schema}\".test_table1 VALUES (1,	N'😀😀',		N'😀😀',		N'😀😀')"
            f"INSERT INTO \"{self.source_schema}\".test_table1 VALUES (2,	N'💩💩',		N'💩💩',		N'💩💩')"
            f"INSERT INTO \"{self.source_schema}\".test_table1 VALUES (3,	N'🍕🍕',	    N'🍕🍕',		N'🍕🍕')"
            f"INSERT INTO \"{self.source_schema}\".test_table1 VALUES (4,	N'🐷🐷',		N'🐷🐷', 		N'🐷🐷')"
            f"INSERT INTO \"{self.source_schema}\".test_table1 VALUES (5,	N'😱😱',	 	N'😱😱',	 	N'😱😱')"
        )
        self.sqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('2')
        self.monitor_page.cdc_tab()
        self.sqldb.cursor.execute(
            f"INSERT INTO \"{self.source_schema}\".test_table VALUES (6,	N'😀😱',	 	N'😀😱',	 	N'😀😱')"
            f"INSERT INTO \"{self.source_schema}\".test_table1 VALUES (6,	N'😀😱',	 	N'😀😱',	 	N'😀😱')"
            f"UPDATE \"{self.source_schema}\".test_table set B=N'😀' , C=N'😀' , D=N'😀' where A=2;"
            f"UPDATE \"{self.source_schema}\".test_table1 set B1=N'😀' , C1=N'😀' , D1=N'😀' where A1=2;"
            f"DELETE FROM \"{self.source_schema}\".test_table WHERE A = 1;"
            f"DELETE FROM \"{self.source_schema}\".test_table1 WHERE A1 = 1;"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('1', '1')
        self.monitor_page.updates_check('1', '1')
        self.monitor_page.delete_check('1', '1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    @unittest.skip("store_changes test good files give a different result every run. Run it manually")
    def test_store_changes_table(self):
        """ Replication task with 'store_changes' functionality. Store changes is a table that contains all the CDC events
            executed from the source database"""

        task_name = "SQL2Oracle_Store_Changes"
        self.task_creation(task_name)
        self.designer_page.enter_task_settings()
        self.task_settings.change_processing()
        self.task_settings.store_changes()
        self.task_settings.ok_button()
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
            f"UPDATE \"{self.source_schema}\".test_table SET B = 'UPDATE' WHERE A = 101;"
            f"DELETE FROM \"{self.source_schema}\".test_table WHERE A = 202;"
        )
        self.sqldb.connection.commit()
        self.monitor_page.inserts_check('1')
        self.monitor_page.updates_check('1')
        self.monitor_page.delete_check('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")


