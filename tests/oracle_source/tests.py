from unittest import TestCase
from browsers.browsers import *
from configurations.config_manager import ConfigurationManager
from databases.sqlserver_db import SQLServerDatabase
from databases.oracle_db import OracleDatabase
from configurations.endpoint_configuration import oracle_source_endpoint, sqlserver_target_endpoint
from utilities.utility_functions import move_file_to_target_dir, compare_files
from replicate_pages import *
from time import sleep

class Oracle_Source_Tests(TestCase):
    """
        This class contains Selenium test cases for Oracle Source in the Qlik Replicate software.
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
        self.logs_location, self.results_location = self.config_manager.oracle_logs_results_path()
        self.oracledb = OracleDatabase(self.config_manager, 'Oracle_DB')
        self.sqldb = SQLServerDatabase(self.config_manager, 'MSSQL_DB')
        self._initialize_web_pages()
        self._initialize_databases()
        self._initialize_web_driver()

    def tearDown(self):
        """ Initialize database connections and schemas for testing.
            Connect to Oracle and SQL Server databases, create and configure schemas. """

        self.oracledb.close()
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
            Connect to Oracle and SQL Server databases, create and configure schemas. """

        self.oracledb.connect()
        self.sqldb.connect()
        self.source_schema, self.target_schema, self.control_schema = self.config_manager.get_default_schemas()
        self.oracledb.drop_all_tables_in_schema(self.source_schema)
        self.sqldb.drop_all_tables_in_schema(self.target_schema)
        self.sqldb.drop_all_tables_in_schema(self.control_schema)
        self.oracledb.drop_user(self.source_schema)
        self.sqldb.drop_schema(self.target_schema)
        self.sqldb.drop_schema(self.control_schema)
        self.oracledb.create_user(self.source_schema)
        self.sqldb.create_schema(self.target_schema)
        self.sqldb.create_schema(self.control_schema)

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
        self.designer_page.choose_source_target(oracle_source_endpoint, sqlserver_target_endpoint)
        self.designer_page.enter_table_selection()
        self.table_selection.choose_source_schema(self.source_schema)
        self.designer_page.enter_task_settings()
        self.task_settings.set_task_settings_general(self.target_schema, self.control_schema)

    def endpoints_creation(self):
        """ Create source and target RDBMS endpoints for Oracle and SQL Server for testing in the Qlik Replicate
            application. """

        self.tasks_general_page.enter_manage_endpoints()
        self.manage_endpoints.create_rdbms_endpoint(oracle_source_endpoint)
        sleep(5)
        self.manage_endpoints.close()
        sleep(5)
        self.tasks_general_page.enter_manage_endpoints()
        self.manage_endpoints.create_rdbms_endpoint(sqlserver_target_endpoint)
        sleep(5)
        self.manage_endpoints.close()

    def test_fl_cdc(self):
        """FL + CDC: basic test with all DMLs"""

        task_name = "Oracle2SQL_FL_CDC"
        self.task_creation(task_name)
        self.oracledb.execute_query(f'CREATE TABLE "{self.source_schema}"."test_table" (COL1 NUMBER NOT NULL, COL2 VARCHAR2(25), PRIMARY KEY (COL1))')

        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (101, \'FL\')')
        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (202, \'FL\')')
        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (303, \'FL\')')
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()

        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (404, \'CDC\')')
        self.oracledb.execute_query(f'UPDATE "{self.source_schema}"."test_table" SET COL2 = \'UPDATE\' WHERE COL1 = 101')
        self.oracledb.execute_query(f'DELETE FROM "{self.source_schema}"."test_table" WHERE COL1 = 202')

        self.monitor_page.inserts_check('1')
        self.monitor_page.updates_check('1')
        self.monitor_page.delete_check('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.sqldb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_several_tables(self):
        """Replication of 3 tables from Oracle database to SQL Server database"""

        task_name = "Oracle2SQL_Several_Tables"
        self.task_creation(task_name)
        for i in range(1,4):
            self.oracledb.execute_query(f'CREATE TABLE "{self.source_schema}"."test_table{str(i)}" (COL1 NUMBER NOT NULL, COL2 VARCHAR2(25), PRIMARY KEY (COL1))')
        for i in range(1, 4):
            self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table{str(i)}" VALUES (101, \'FL\')')
            self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table{str(i)}" VALUES (202, \'FL\')')
            self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table{str(i)}" VALUES (303, \'FL\')')
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('3')
        self.monitor_page.cdc_tab()
        for i in range(1, 4):
            self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table{str(i)}" VALUES (404, \'CDC\')')
            self.oracledb.execute_query(f'UPDATE "{self.source_schema}"."test_table{str(i)}" SET COL2 = \'UPDATE\' WHERE COL1 = 101')
            self.oracledb.execute_query(f'DELETE FROM "{self.source_schema}"."test_table{str(i)}" WHERE COL1 = 202')
        self.monitor_page.inserts_check('1', '1', '1')
        self.monitor_page.updates_check('1', '1', '1')
        self.monitor_page.delete_check('1', '1', '1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.sqldb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_char_columns(self):
        """Replication of a table with 'chars' datatype from SOracle source to SQL Server target"""

        task_name = "Oracle2SQL_Chars_Datatype"
        self.task_creation(task_name)
        self.oracledb.execute_query(f'CREATE TABLE "{self.source_schema}"."test_table" (COL1 int primary key, char_1 char(36), nchar_1 nchar(36), varchar2_1 varchar2(36), nvarchar2_1 nvarchar2(36))')
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (1, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (2, '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (3, 'a', 'a', 'a', 'a')")
        self.oracledb.execute_query(f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (4, '', '', '', '')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (5, ' ', ' ', ' ', ' ')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (6, ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (7, 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (8, null,null,null,null)")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (9, CHR(49) || CHR(0) || CHR(48) || CHR(49), CHR(49) || CHR(0) || CHR(48) || CHR(49), CHR(49) || CHR(0) || CHR(48) || CHR(49), CHR(49) || CHR(0) || CHR(48) || CHR(49))")
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (10, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (11, '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (12, 'a', 'a', 'a', 'a')")
        self.oracledb.execute_query(f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (13, '', '', '', '')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (14, ' ', ' ', ' ', ' ')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (15, ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (16, 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ')")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (17, null,null,null,null)")
        self.oracledb.execute_query(
            f"INSERT INTO \"{self.source_schema}\".\"test_table\" VALUES (18, CHR(49) || CHR(0) || CHR(48) || CHR(49), CHR(49) || CHR(0) || CHR(48) || CHR(49), CHR(49) || CHR(0) || CHR(48) || CHR(49), CHR(49) || CHR(0) || CHR(48) || CHR(49))")
        self.oracledb.execute_query(f'DELETE FROM "{self.source_schema}"."test_table" WHERE COL1 = 5')
        self.monitor_page.inserts_check('9')
        self.monitor_page.delete_check('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.sqldb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_number_columns(self):
        """Replication of a table with 'integer' datatype columns from SQL Server source to Oracle target"""

        task_name = "Oracle2SQL_Number_Datatype"
        self.task_creation(task_name)
        self.oracledb.execute_query(
            f'CREATE TABLE "{self.source_schema}"."test_table" (COL1 int primary key, number_1 number, number_2 NUMBER(10,5), number_3 number (7), number_4 number(10), number_5 number(5), number_6 number(3), number_7 number(38), number_8 Number(5,-1), number_9 Number(5,5))')
        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (1, 1.000E+100, 12345.67890, 1234567, 1234567890, 12345, 123, 1, 12345, 0.12345)')
        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (2, 8884407, 2 ,9999999, 1, 1, 1,99999999999999999999999999999999999999, 1234, 0.1234)')
        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (3, 0, 1.23456, 0, 9999999999, 99999, 999, 0, 12, 0.12)')
        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (4, null, null, null, null, null, null, null, null, null)')
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('1')
        self.monitor_page.cdc_tab()
        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (5, 1.000E+100, 12345.67890, 1234567, 1234567890, 12345, 123, 1, 12345, 0.12345)')
        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (6, 8884407, 2 ,9999999, 1, 1, 1,99999999999999999999999999999999999999, 1234, 0.1234)')
        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (7, 0, 1.23456, 0, 9999999999, 99999, 999, 0, 12, 0.12)')
        self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table" VALUES (8, null, null, null, null, null, null, null, null, null)')
        self.oracledb.execute_query(f'DELETE FROM "{self.source_schema}"."test_table" WHERE COL1 = 2')
        self.monitor_page.inserts_check('4')
        self.monitor_page.delete_check('1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.sqldb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")

    def test_table_DDL(self):
        """FL + CDC task with table related DDL statements (ADD DROP, ALTER) """

        task_name = "Oracle2SQL_Table_DDL"
        self.task_creation(task_name)
        for i in range(1,3):
            self.oracledb.execute_query(f'CREATE TABLE "{self.source_schema}"."test_table{str(i)}" (COL1 NUMBER NOT NULL, COL2 VARCHAR2(25), PRIMARY KEY (COL1))')
        for i in range(1, 3):
            self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table{str(i)}" VALUES (101, \'FL\')')
            self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table{str(i)}" VALUES (202, \'FL\')')
            self.oracledb.execute_query(f'INSERT INTO "{self.source_schema}"."test_table{str(i)}" VALUES (303, \'FL\')')
        self.designer_page.run_new_task()
        self.designer_page.start_task_wait()
        self.monitor_page.wait_for_fl('2')
        self.monitor_page.cdc_tab()
        self.oracledb.execute_query(
            f'CREATE TABLE "{self.source_schema}"."test_table3" (COL1 NUMBER NOT NULL, COL2 VARCHAR2(25), PRIMARY KEY (COL1))')
        self.monitor_page.ddl_check('0', '0', '1')
        self.oracledb.execute_query(f'DROP TABLE "{self.source_schema}"."test_table2"')
        self.monitor_page.ddl_check('0', '1', '1')
        self.oracledb.execute_query(
            f'ALTER TABLE "{self.source_schema}"."test_table1" RENAME TO WOW')
        self.monitor_page.ddl_check('0', '1', '1', '1')
        self.monitor_page.stop_task()
        self.monitor_page.stop_task_wait()
        move_file_to_target_dir(self.config_manager.source_tasklog_path(),
                                self.logs_location, f"reptask_{task_name}.log")
        self.common_functions.navigate_to_main_page('tasks')
        self.tasks_general_page.delete_task(task_name)
        self.sqldb.export_schema_data_to_csv(self.target_schema, f"{self.results_location}\\{task_name}.csv")
        compare_files(f"{self.results_location}\\{task_name}.good", f"{self.results_location}\\{task_name}.csv")


