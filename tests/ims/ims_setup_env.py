import faulthandler
import shutil

faulthandler.disable()

import pytest
from pathlib import Path
from types import SimpleNamespace
from configurations.config_manager import ConfigurationManager
from browsers.browsers import get_webdriver
from databases.ims_db import IMSDatabase
from databases.oracle_db import OracleDatabase
from replicate_pages import *
from utilities.ims_dbd_mapping import TEST_DBD_MAP

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

# 3️⃣ IMS DB (Session level) - depends on jvm_manager
@pytest.fixture
def ims(config_manager, request):
    print("💾 IMS setup")
    test_file = Path(request.fspath)
    test_dir_name = test_file.parent.name
    dbd_name = TEST_DBD_MAP.get(test_dir_name)
    if not dbd_name:
        raise ValueError(f"No DBD mapping found for test folder: {test_dir_name}")
    dbd_file = Path(__file__).resolve().parents / "dbd_files" / dbd_name
    if not dbd_file.exists():
        raise FileNotFoundError(f"DBD file not found: {dbd_file}")
    ims_metadata_dir = Path(config_manager.get_section('IMS_DB')['xmlMetadataLocation']) / "DBD"
    ims_metadata_dir.mkdir(parents=True, exist_ok=True)
    dst_file = ims_metadata_dir / "IMSDEV.dbd"
    shutil.copy(str(dbd_file), str(dst_file))
    print(f"Copied DBD for test folder '{test_dir_name}': {dbd_file} → {dst_file}")

    shutil.copy(dbd_file, ims_metadata_dir / dbd_file.name)
    ims_db = IMSDatabase(config_manager, 'IMS_DB')
    ims_db.connect()
    yield ims_db, dbd_file
    print("🧹 IMS teardown")
    ims_db.close()

# 4️⃣ Oracle DB (Session level) - depends on jvm_manager
@pytest.fixture(scope="session")
def oracle(config_manager):
    print("🛢️ Oracle setup")
    # This fixture now assumes the JVM is already running.
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
def reset_database_env(default_schemas, oracle):
    source_schema, target_schema, control_schema = default_schemas
    print("🔁 Resetting database environment (per test)")
    oracle.drop_all_tables_in_schema(target_schema)
    oracle.drop_all_tables_in_schema(control_schema)
    oracle.drop_user(target_schema)
    oracle.drop_user(control_schema)
    oracle.create_user(target_schema)
    oracle.create_user(control_schema)


# 🌐 Setup browser UI
@pytest.fixture(scope="session")
def setup_browser(driver, replicate_pages):
    print("🌍 Browser UI setup")
    replicate_pages.replicate_actions.open_replicate_software()
    replicate_pages.replicate_actions.set_windows_size()
    driver.implicitly_wait(3)
    replicate_pages.replicate_actions.loader_icon_opening_replicate()


# 🧪 Per-test fixture to set up full test environment
@pytest.fixture
def ims_test(request, ims, oracle, replicate_pages, default_schemas, reset_database_env, setup_browser):
    ims_db , dbd_file = ims
    test_dir = Path(request.fspath).parent

    env = SimpleNamespace(
        **replicate_pages.__dict__,
        ims_db=ims,
        oracle_db=oracle,
        target_schema=default_schemas[1],
        control_schema=default_schemas[2],
        ims_source_name=None,
        oracle_target_name=None,
        task_name=None,
        dbd_file = dbd_file,  # full path to the DBD file
        test_dir = str(test_dir),  # base test folder as string
        task_logs_dir = str(test_dir / "task_logs"),  # full string path to task_logs
        good_files_dir = str(test_dir / "good_files")  # now available as string
    )
    yield env
    replicate_pages.replicate_actions.delete_task_endpoint(
        env.task_name, env.ims_source_name, env.oracle_target_name
    )
