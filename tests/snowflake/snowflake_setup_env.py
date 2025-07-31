import pytest
from types import SimpleNamespace
from configurations.config_manager import ConfigurationManager
from browsers.browsers import get_webdriver
from databases.snowflake_db import SnowflakeDatabase
from databases.oracle_db import OracleDatabase
from replicate_pages import *


# 1️⃣ Config Manager (Session level)
@pytest.fixture(scope="session")
def config_manager():
    print("🛠️ Config Manager setup")
    return ConfigurationManager(r"C:\Users\JUJ\PycharmProjects\qlik-replicate-automation\configurations\config.ini")

# 2️⃣ WebDriver (Session level)
@pytest.fixture(scope="session")
def driver(config_manager):
    print("🌐 WebDriver setup")
    driver = get_webdriver(config_manager)
    yield driver
    print("🧹 WebDriver teardown")
    driver.quit()


# 3️⃣ Snowflake DB (Session level)
@pytest.fixture(scope="session")
def snowflake(config_manager):
    print("❄️ Snowflake setup")
    snowflake_db = SnowflakeDatabase(config_manager, 'Snowflake_DB')
    snowflake_db.connect()
    yield snowflake_db
    print("🧹 Snowflake teardown")
    snowflake_db.disconnect()


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


# 🔄 Clean database state before each test
@pytest.fixture
def reset_database_env(default_schemas, oracle, snowflake):
    source_schema, target_schema, control_schema = default_schemas
    print("🔁 Resetting database environment (per test)")
    oracle.drop_all_tables_in_schema(target_schema)
    oracle.drop_all_tables_in_schema(control_schema)
    snowflake.drop_schema(source_schema)
    oracle.drop_user(target_schema)
    oracle.drop_user(control_schema)
    snowflake.create_schema(source_schema)
    oracle.create_user(target_schema)
    oracle.create_user(control_schema)


#🌐 Setup browser UI
@pytest.fixture(scope="session")
def setup_browser(driver, replicate_pages):
    print("🌍 Browser UI setup")
    replicate_pages.replicate_actions.open_replicate_software()
    replicate_pages.replicate_actions.set_windows_size()
    driver.implicitly_wait(3)
    replicate_pages.replicate_actions.loader_icon_opening_replicate()

# Per-test fixture to set up full test environment
@pytest.fixture
def snow_test(snowflake, oracle, replicate_pages, default_schemas, reset_database_env, setup_browser):
    env = SimpleNamespace(
        **replicate_pages.__dict__,
        snowflake=snowflake,
        oracle=oracle,
        source_schema=default_schemas[0],
        target_schema=default_schemas[1],
        control_schema=default_schemas[2],
        snowflake_source_name= None,
        oracle_target_name= None,
        task_name=None
    )

    yield env

    replicate_pages.replicate_actions.delete_task_endpoint(env.task_name, env.snowflake_source_name, env.oracle_target_name)
    # replicate_pages.replicate_actions.open_replicate_software()
    # replicate_pages.replicate_actions.loader_icon_opening_replicate()


def create_task(snow_test: SimpleNamespace, task_name: str):
    snow_test.tasks_general_page.enter_manage_endpoints()
    snow_test.snowflake_source_name = snow_test.manage_endpoints.random_endpoint_name('Snowflake_DB')
    snow_test.oracle_target_name = snow_test.manage_endpoints.random_endpoint_name('Oracle_DB')
    snow_test.manage_endpoints.create_snowflake_source_endpoint(snow_test.snowflake_source_name)
    snow_test.manage_endpoints.create_oracle_target_endpoint(snow_test.oracle_target_name)
    snow_test.manage_endpoints.close()
    snow_test.tasks_general_page.create_new_task()
    new_task_name = snow_test.new_task_page.new_task_creation(task_name)
    snow_test.replicate_actions.task_data_loader()
    snow_test.designer_page.choose_source_target(snow_test.snowflake_source_name, snow_test.oracle_target_name)
    snow_test.designer_page.enter_table_selection()
    snow_test.table_selection.choose_source_schema()
    snow_test.designer_page.enter_task_settings()
    snow_test.task_settings.set_task_settings_general()
    snow_test.task_name = new_task_name
    return new_task_name

