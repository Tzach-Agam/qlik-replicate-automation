from unittest import TestCase
from browsers.browsers import *
from configurations.config_manager import ConfigurationManager
from databases.mysql_db import MySQLDatabase
from databases.oracle_db import OracleDatabase
from configurations.endpoint_configuration import mysql_source_endpoint, oracle_target_endpoint
from utilities.utility_functions import move_file_to_target_dir, compare_files
from replicate_pages import *
from time import sleep

class MySQL_Source_Tests(TestCase):
    """
    This class contains Selenium test cases for MySQL Source in the Qlik Replicate software.
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
        self.logs_location, self.results_location = self.config_manager.mysql_logs_results_path()
        self.mysqldb = MySQLDatabase(self.config_manager, 'MySQL_db')
        self.oracledb = OracleDatabase(self.config_manager, 'Oracle_db')
        self._initialize_web_pages()
        self._initialize_databases()
        self._initialize_web_driver()

    def tearDown(self):
        """ Tear down the test environment.
            Drop replication constraints and close database connections. """

        self.mysqldb.close()
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
             Connect to MySQL and Oracle databases, create and configure schemas. """

        self.mysqldb.connect()
        self.oracledb.connect()
        self.source_schema, self.target_schema, self.control_schema = self.config_manager.get_default_schemas()
        self.mysqldb.drop_all_tables_in_schema(self.source_schema)
        self.oracledb.drop_all_tables_in_schema(self.target_schema)
        self.oracledb.drop_all_tables_in_schema(self.control_schema)
        self.mysqldb.drop_schema(self.source_schema)
        self.oracledb.drop_user(self.target_schema)
        self.oracledb.drop_user(self.control_schema)
        self.mysqldb.create_schema(self.source_schema)
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
        self.designer_page.choose_source_target(mysql_source_endpoint, oracle_target_endpoint)
        self.designer_page.enter_table_selection()
        self.table_selection.choose_source_schema(self.source_schema)
        self.designer_page.enter_task_settings()
        self.task_settings.set_task_settings_general(self.target_schema, self.control_schema)

    def endpoints_creation(self):
        """ Create source and target RDBMS endpoints for MySQL and Oracle  for testing in the Qlik Replicate
            application. """

        self.tasks_general_page.enter_manage_endpoints()
        self.manage_endpoints.create_rdbms_endpoint(mysql_source_endpoint)
        sleep(5)
        self.manage_endpoints.close()
        sleep(5)
        self.tasks_general_page.enter_manage_endpoints()
        self.manage_endpoints.create_rdbms_endpoint(oracle_target_endpoint)
        sleep(5)
        self.manage_endpoints.close()

    def test_fl_cdc(self):
        """FL + CDC: basic test with all DMLs"""

        task_name = "MySQL2Oracle_FL_CDC"
        self.task_creation(task_name)
        self.mysqldb.execute_query(f'CREATE TABLE {self.source_schema}.test_table (A int primary key, B varchar(20));')
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (101, 'FL');")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (202, 'FL');")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (303, 'FL');")
        self.mysqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (404, 'FL');")
        self.mysqldb.execute_query(f"UPDATE {self.source_schema}.test_table SET B = 'UPDATE' WHERE A = 101;")
        self.mysqldb.execute_query(f"DELETE FROM {self.source_schema}.test_table WHERE A = 202;")
        self.mysqldb.connection.commit()
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

        task_name = "MySQL2Oracle_Several_Tables"
        self.task_creation(task_name)
        for i in range(1, 4):
            self.mysqldb.execute_query(
                f'CREATE TABLE {self.source_schema}.test_table{str(i)} (A int primary key, B varchar(20));')
        for i in range(1, 4):
            self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table{str(i)} VALUES (101, 'FL');")
            self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table{str(i)} VALUES (202, 'FL');")
            self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table{str(i)} VALUES (303, 'FL');")
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('3')
        self.monitor_page.cdc_tab()
        for i in range(1, 4):
            self.mysqldb.execute_query(
                f"INSERT INTO {self.source_schema}.test_table{str(i)} VALUES (404, 'CDC');")
            self.mysqldb.execute_query(
                f"UPDATE {self.source_schema}.test_table{str(i)} SET B = 'UPDATE' WHERE A = 101;")
            self.mysqldb.execute_query(f"DELETE FROM {self.source_schema}.test_table{str(i)} WHERE A = 202;")
        self.monitor_page.inserts_check('1', '1', '1')
        self.monitor_page.updates_check('1', '1', '1')
        self.monitor_page.delete_check('1', '1', '1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_truncate_table(self):
        """FL + CDC with TRUNCATE TABLE DDL"""

        task_name = "MySQL2Oracle_Truncate_table"
        self.task_creation(task_name)
        self.mysqldb.execute_query(f'CREATE TABLE {self.source_schema}.test_table (a int primary key,b int,c char(20),d varchar(20));')
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (1,55,'abcde','abcde');")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (2,55,'abcde','abcde');")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (3,55,'abcde','abcde');")
        self.mysqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (4,55,'abcde','abcde');")
        self.monitor_page.inserts_check('1')
        self.mysqldb.execute_query(f"TRUNCATE TABLE {self.source_schema}.test_table;")
        self.monitor_page.ddl_check('1')
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (5,55,'abcde','abcde');")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (6,55,'abcde','abcde');")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (7,55,'abcde','abcde');")
        self.monitor_page.inserts_check('4')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_uints_datatype(self):
        """Replication of a table with 'UINTS' datatype that are unique to MySQL from MySQL source to Oracle target"""

        task_name = "MySQL2Oracle_uints_Datatype"
        self.task_creation(task_name)
        self.mysqldb.execute_query(f'CREATE TABLE {self.source_schema}.test_table (a int primary key, TINYINT_1 TINYINT unsigned, SMALLINT_1 SMALLINT unsigned, MEDIUMINT_1 MEDIUMINT unsigned, INT_1 INT unsigned, BIGINT_1 BIGINT unsigned);')
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table VALUES (1, 25, null, 16777215, -null, 9223372036854775807);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table VALUES (2, null, 65535, null, 4294967295, null);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table VALUES (3, null, 32767, null, 2147483647, null);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table VALUES (4, 12, null, 8388607, null, 9223372036854775807);")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (5, 0, 0, 0, 0, 0);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table VALUES (6, null, null, null, null, null);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table VALUES (7, 12, 32767, 8388608, 2147483648, 9223372036854775807);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table VALUES (8, 12, null, 16777, null, 184467);")
        self.mysqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table values (9, 25, null, 16777215, -null, 9223372036854775807);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table values (10, null, 65535, null, 4294967295, null);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table values (11, null, 32767, null, 2147483647, null);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table values (12, 12, null, 8388607, null, 9223372036854775807);")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table values (13, 0, 0, 0, 0, 0);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table values (14, null, null, null, null, null);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table values (15, 12, 32767, 8388608, 2147483648, 9223372036854775807);")
        self.mysqldb.execute_query(
            f"INSERT INTO {self.source_schema}.test_table values (16, 12, null, 16777, null, 184467);")
        self.mysqldb.execute_query(
            f"UPDATE {self.source_schema}.test_table SET TINYINT_1=25, SMALLINT_1=65535, MEDIUMINT_1=16777215, INT_1=4294967295, BIGINT_1=9223372036854775807 WHERE a=1;")
        self.mysqldb.execute_query(
            f"UPDATE {self.source_schema}.test_table SET TINYINT_1=12, SMALLINT_1=32768, MEDIUMINT_1=8388608, INT_1=2147483647, BIGINT_1=9223372036854775807 WHERE a=2;")
        self.mysqldb.execute_query(f"DELETE FROM {self.source_schema}.test_table WHERE a=7;")
        self.mysqldb.execute_query(f"DELETE FROM {self.source_schema}.test_table WHERE a=8;")
        self.mysqldb.connection.commit()
        self.monitor_page.inserts_check('8')
        self.monitor_page.updates_check('2')
        self.monitor_page.delete_check('2')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.oracledb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_bit_datatype(self):
        """Replication of a table with 'BIT' datatype from MySQL source to Oracle target"""

        task_name = "MySQL2Oracle_bit_Datatype"
        self.task_creation(task_name)
        self.mysqldb.execute_query(f'CREATE TABLE {self.source_schema}.test_table (a int primary key, bit_1 bit, bit_2 bit(64));')
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (1,0,  10110110110001101010);")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (2,1, 0000000010110110110001101010);")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (3, null, null);")
        self.mysqldb.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (4,0,  10110110110001101010);")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (5,1, 0000000010110110110001101010);")
        self.mysqldb.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (6, null, null);")
        self.mysqldb.execute_query(f'UPDATE {self.source_schema}.test_table SET bit_1=1, bit_2= 01010101 WHERE a=1;')
        self.mysqldb.execute_query(f"DELETE FROM {self.source_schema}.test_table WHERE a=2;")
        self.mysqldb.connection.commit()
        self.monitor_page.inserts_check('3')
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