import inspect
from pathlib import Path
import pytest
from types import SimpleNamespace
from configurations.config_manager import ConfigurationManager
from browsers.browsers import get_webdriver
from databases.sqlserver_db import SQLServerDatabase
from databases.oracle_db import OracleDatabase
from replicate_pages import *
from utilities.utility_functions import move_file_to_target_dir, compare_files, log_finder


# 1️⃣ Config Manager (Session level)
@pytest.fixture(scope="session")
def config_manager():
    print("🛠️ Config Manager setup")
    project_root = Path(__file__).resolve().parents[2]
    config_path = project_root / "configurations" / "config.ini"
    return ConfigurationManager(str(config_path))

# 2️⃣ WebDriver (Session level)
@pytest.fixture(scope="session")
def driver(config_manager):
    print("🌐 WebDriver setup")
    driver = get_webdriver(config_manager)
    yield driver
    print("🧹 WebDriver teardown")
    driver.quit()

# 3️⃣ SQL Server DB (Session level)
@pytest.fixture(scope="session")
def mssql(config_manager):
    print("SQL Server setup")
    mssql_db = SQLServerDatabase(config_manager, 'MSSQL_DB')
    mssql_db.connect()
    yield mssql_db
    print("🧹 SQL Server teardown")
    mssql_db.close()

# 4️⃣ Oracle DB (Session level)
@pytest.fixture(scope="session")
def oracle(config_manager):
    print("🛢️ Oracle setup")
    oracle_db = OracleDatabase(config_manager, 'Oracle_DB')
    oracle_db.connect()
    yield oracle_db
    print("🧹 Oracle teardown")
    oracle_db.close()

# 5️⃣ Page Object Models (Session level)
@pytest.fixture(scope="session")
def replicate_pages(driver, config_manager):
    print("📄 Replicate Pages setup")
    return SimpleNamespace(
        replicate_actions=ReplicateCommonActions(driver, config_manager),
        tasks_general_page=TasksPage(driver),
        new_task_page=NewTaskPage(driver),
        manage_endpoints=ManageEndpoints(driver, config_manager),
        task_settings=TaskSettings(driver, config_manager),
        table_selection=TableSelection(driver, config_manager),
        designer_page=DesignerPage(driver),
        monitor_page=MonitorPage(driver)
    )

# 6️⃣ Default schemas (Session level)
@pytest.fixture(scope="session")
def default_schemas(config_manager):
    print("📦 Acquiring default schemas")
    return config_manager.get_default_schemas()

# 6️⃣ Default table (Session level)
@pytest.fixture(scope="session")
def default_tables(config_manager):
    print("📦 Acquiring default table")
    return config_manager.get_default_tables()

#🌐 Setup browser UI
@pytest.fixture(scope="session")
def setup_browser(driver, replicate_pages):
    print("🌍 Browser UI setup")
    replicate_pages.replicate_actions.open_replicate_software()
    replicate_pages.replicate_actions.set_windows_size()
    driver.implicitly_wait(3)
    replicate_pages.replicate_actions.loader_icon_opening_replicate()

# 🔄 Clean database state before each test
@pytest.fixture
def reset_database_env(default_schemas, default_tables, mssql, oracle):
    source_schema, target_schema, control_schema = default_schemas
    default_table, sync_table = default_tables
    print("🔁 Resetting database environment (per test)")
    mssql.drop_all_tables_in_schema(source_schema)
    mssql.drop_schema(source_schema)
    oracle.drop_all_tables_in_schema(target_schema)
    oracle.drop_all_tables_in_schema(control_schema)
    oracle.drop_user(target_schema)
    oracle.drop_user(control_schema)
    mssql.create_schema(source_schema)
    oracle.create_user(target_schema)
    oracle.create_user(control_schema)
    mssql.create_table(source_schema, sync_table, ["sync_col int IDENTITY(1,1) primary key"])

# Per-test fixture to set up full test environment
@pytest.fixture
def mssql_test(request, config_manager, mssql, oracle, replicate_pages, default_schemas, default_tables, reset_database_env, setup_browser):
    test_dir = Path(request.fspath).parent  # <-- get test file folder as string

    env = SimpleNamespace(
        config=config_manager,
        **replicate_pages.__dict__,
        mssql_db=mssql,
        oracle_db=oracle,
        source_schema=default_schemas[0],
        target_schema=default_schemas[1],
        control_schema=default_schemas[2],
        default_table=default_tables[0],
        sync_table=default_tables[1],
        mssql_source_name=None,
        oracle_target_name=None,
        task_name=None,
        test_dir=str(test_dir),  # base test folder as string
        task_logs_dir=str(test_dir / "task_logs"),  # full string path to task_logs
        good_files_dir=str(test_dir / "good_files")  # now available as string
    )

    yield env

    mssql.drop_replication_constraint()
    replicate_pages.replicate_actions.delete_task_endpoint(env.task_name, env.mssql_source_name, env.oracle_target_name)
    replicate_pages.replicate_actions.open_replicate_software()
    replicate_pages.replicate_actions.loader_icon_opening_replicate()

def create_task(mssql_test: SimpleNamespace, task_name: str):
    mssql_test.tasks_general_page.enter_manage_endpoints()
    mssql_test.mssql_source_name = mssql_test.manage_endpoints.random_endpoint_name('MSSQL_DB')
    mssql_test.oracle_target_name = mssql_test.manage_endpoints.random_endpoint_name('Oracle_DB')
    mssql_test.manage_endpoints.create_sql_server_source_endpoint(mssql_test.mssql_source_name)
    mssql_test.manage_endpoints.create_oracle_target_endpoint(mssql_test.oracle_target_name)
    mssql_test.manage_endpoints.close()
    mssql_test.tasks_general_page.create_new_task()
    new_task_name = mssql_test.new_task_page.new_task_creation(task_name)
    mssql_test.replicate_actions.task_data_loader()
    mssql_test.designer_page.choose_source_target(mssql_test.mssql_source_name, mssql_test.oracle_target_name)
    mssql_test.designer_page.enter_table_selection()
    mssql_test.table_selection.choose_source_schema()
    mssql_test.designer_page.enter_task_settings()
    mssql_test.task_settings.set_task_settings_general()
    mssql_test.task_name = new_task_name
    return new_task_name

