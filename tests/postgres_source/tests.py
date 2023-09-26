from unittest import TestCase
from browsers.browsers import *
from configurations.config_manager import ConfigurationManager
from databases.postgres_db import PostgreSQLDatabase
from databases.oracle_db import OracleDatabase
from configurations.endpoint_configuration import postgres_source_endpoint, oracle_target_endpoint
from utilities.utility_functions import move_file_to_target_dir, compare_files
from replicate_pages import *
from time import sleep

class Postgres_Source_Tests(TestCase):
    """
    This class contains Selenium test cases for PostgreSQL Source in the Qlik Replicate software.
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
        self.logs_location, self.results_location = self.config_manager.postgres_logs_results_path()
        self.postgres_db = PostgreSQLDatabase(self.config_manager, 'Postgres_db')
        self.oracledb = OracleDatabase(self.config_manager, 'Oracle_db')
        self._initialize_web_pages()
        self._initialize_databases()
        self._initialize_web_driver()

    def tearDown(self):
        """ Tear down the test environment.
            Drop replication constraints and close database connections. """

        self.postgres_db.close()
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
            Connect to PostgreSQL and Oracle databases, create and configure schemas. """

        self.postgres_db.connect()
        self.oracledb.connect()
        self.source_schema, self.target_schema, self.control_schema = self.config_manager.get_default_schemas()
        self.postgres_db.drop_all_tables_in_schema(self.source_schema)
        self.oracledb.drop_all_tables_in_schema(self.target_schema)
        self.oracledb.drop_all_tables_in_schema(self.control_schema)
        self.postgres_db.drop_schema(self.source_schema)
        self.oracledb.drop_user(self.target_schema)
        self.oracledb.drop_user(self.control_schema)
        self.postgres_db.create_schema(self.source_schema)
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
        self.designer_page.choose_source_target(postgres_source_endpoint, oracle_target_endpoint)
        self.designer_page.enter_table_selection()
        self.table_selection.choose_source_schema(self.source_schema)
        self.designer_page.enter_task_settings()
        self.task_settings.set_task_settings_general(self.target_schema, self.control_schema)

    def endpoints_creation(self):
        """ Create source and target RDBMS endpoints for PostgreSQL and Oracle  for testing in the Qlik Replicate
            application. """

        self.tasks_general_page.enter_manage_endpoints()
        self.manage_endpoints.create_rdbms_endpoint(postgres_source_endpoint)
        sleep(5)
        self.manage_endpoints.close()
        sleep(5)
        self.tasks_general_page.enter_manage_endpoints()
        self.manage_endpoints.create_rdbms_endpoint(oracle_target_endpoint)
        sleep(5)
        self.manage_endpoints.close()

    def test_fl_cdc(self):
        """FL + CDC: basic test with all DMLs"""

        task_name = "Postgres2Oracle_FL_CDC"
        self.task_creation(task_name)
        self.postgres_db.execute_query(f'CREATE TABLE {self.source_schema}.test_table (A int primary key, B varchar(20));')
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (101, 'FL');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (202, 'FL');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (303, 'FL');")
        self.postgres_db.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (404, 'FL');")
        self.postgres_db.execute_query(f"UPDATE {self.source_schema}.test_table SET B = 'UPDATE' WHERE A = 101;")
        self.postgres_db.execute_query(f"DELETE FROM {self.source_schema}.test_table WHERE A = 202;")
        self.postgres_db.connection.commit()
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
        """Replication of 3 tables from PostgreSQL database to Oracle database"""

        task_name = "Postgres2Oracle_Several_Tables"
        self.task_creation(task_name)
        for i in range(1, 4):
            self.postgres_db.execute_query(
                f'CREATE TABLE {self.source_schema}.test_table{str(i)} (A int primary key, B varchar(20));')
        for i in range(1, 4):
            self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table{str(i)} VALUES (101, 'FL');")
            self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table{str(i)} VALUES (202, 'FL');")
            self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table{str(i)} VALUES (303, 'FL');")
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('3')
        self.monitor_page.cdc_tab()
        for i in range(1, 4):
            self.postgres_db.execute_query(
                f"INSERT INTO {self.source_schema}.test_table{str(i)} VALUES (404, 'CDC');")
            self.postgres_db.execute_query(
                f"UPDATE {self.source_schema}.test_table{str(i)} SET B = 'UPDATE' WHERE A = 101;")
            self.postgres_db.execute_query(f"DELETE FROM {self.source_schema}.test_table{str(i)} WHERE A = 202;")
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

    def test_date_time_datatype(self):
        """Replication of a table with date, time and datetime datatypes from PostgreSQL"""

        task_name = "Postgres2Oracle_Datetime"
        self.task_creation(task_name)
        self.postgres_db.execute_query(
            f'CREATE TABLE {self.source_schema}.test_table (id int primary key, DATE_1 date,TIME_1 time,TIME_WITH_TIMEZONE_1 time with time zone, TIMESTAMP_1 timestamp);')
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (1,'2000-01-01','8:59:59','2000-01-01 00:00:00+02','1970-01-02 00:00:01');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (2,'2000-12-31','8:59:59', '2000-12-31 23:59:59+02','09/12/2015 09:23:35');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (3,'1111-11-11','11:12', '1988-09-18 11:20:12+02','2000-09-18 06:14:07');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (4,'1111-11-11','1010', '1001-12-31 00:00:59+02','2038-01-02 03:14:07');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (5,'1111-11-11','00:00:00', '2000-12-31 23:59:59+02','2038-01-02 03:14:07');")
        self.postgres_db.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (6, null, null, null, null);")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (7,'2000-12-31','8:59:59', '2000-12-31 23:59:59+02','09/12/2015 09:23:35');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (8,'1111-11-11','11:12', '1988-09-18 11:20:12+02','2000-09-18 06:14:07');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (9,'1111-11-11','1010', '1001-12-31 00:00:59+02','2038-01-02 03:14:07');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (10,'1111-11-11','00:00:00', '2000-12-31 23:59:59+02','2038-01-02 03:14:07');")
        self.postgres_db.execute_query(f"UPDATE {self.source_schema}.test_table set TIMESTAMP_1='09/12/2015 09:23:35' WHERE id = 10;")
        self.postgres_db.execute_query(f"DELETE FROM {self.source_schema}.test_table WHERE id = 9;")
        self.postgres_db.connection.commit()
        self.monitor_page.inserts_check('5')
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

    def test_boolean_datatype(self):
        """Replication of a table with boolean datatype from PostgreSQL"""


        task_name = "Postgres2Oracle_Boolean"
        self.task_creation(task_name)
        self.postgres_db.execute_query(
            f'CREATE TABLE {self.source_schema}.test_table (PK int primary key,COL_1 boolean,COL_2 boolean);')
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (1,'TRUE','FALSE');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (2,'t','f');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (3,'true','false');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (4,'y','n');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (5,'yes','no');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (6,'on','off');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (7, null,null);")
        self.postgres_db.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (8,'TRUE','FALSE');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (9,'t','f');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (10,'true','false');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (11,'y','n');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (12,'yes','no');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (13,'on','off');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (14, null,null);")
        self.postgres_db.execute_query(f"UPDATE {self.source_schema}.test_table SET COL_1='TRUE',COL_2='TRUE' WHERE PK=1;")
        self.postgres_db.execute_query(f"DELETE FROM {self.source_schema}.test_table WHERE PK=5;")
        self.postgres_db.connection.commit()
        self.monitor_page.inserts_check('7')
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

    def test_real_datatype(self):
        """Replication of a table with REAL datatype from PostgreSQL"""

        task_name = "Postgres2Oracle_Real"
        self.task_creation(task_name)
        self.postgres_db.execute_query(
            f'CREATE TABLE {self.source_schema}.test_table (PK int primary key, COL_1 real,COL_2 double precision ,COL_3 money);')
        self.postgres_db.execute_query(
            f"INSERT INTO {self.source_schema}.test_table VALUES (1,123456,123456789123456,-1000);")
        self.postgres_db.execute_query(
            f"INSERT INTO {self.source_schema}.test_table VALUES (2,123456,123456789123456,1000);")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (3,0,0,0.0);")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (4, 0, 0, '');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (5, 0, 0, ' ');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (6,null,null,null);")
        self.postgres_db.connection.commit()
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.postgres_db.execute_query(
            f"INSERT INTO {self.source_schema}.test_table VALUES (8,123456,123456789123456,-1000);")
        self.postgres_db.execute_query(
            f"INSERT INTO {self.source_schema}.test_table VALUES (9,123456,123456789123456,1000);")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (10,0,0,0.0);")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (11, 0, 0, '');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (12, 0, 0, ' ');")
        self.postgres_db.execute_query(f"INSERT INTO {self.source_schema}.test_table VALUES (13,null,null,null);")
        self.postgres_db.execute_query(
            f"UPDATE {self.source_schema}.test_table SET COL_1=1,COL_2=1, COL_3=1 WHERE PK=1;")
        self.postgres_db.execute_query(
            f"UPDATE {self.source_schema}.test_table SET COL_1=1991,COL_2=1991, COL_3=1991 WHERE PK=4;")
        self.postgres_db.execute_query(f"DELETE FROM {self.source_schema}.test_table WHERE PK=5;")
        self.postgres_db.connection.commit()
        self.monitor_page.inserts_check('6')
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