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
from databases.sqlserver_db import SQLServerDatabase
from replicate_pages import *
from utilities.ims_dbd_mapping import TEST_DBD_MAP
from utilities.utility_functions import move_file_to_target_dir, compare_files, log_finder

# ----------------------------------------
# 0️⃣ Read config once
# ----------------------------------------
CONFIG = ConfigurationManager(str(Path(__file__).resolve().parents[2] / "configurations" / "config.ini"))
ENABLED_TARGETS = CONFIG.get_enabled_targets()

# ----------------------------------------
# 1️⃣ Config Manager
# ----------------------------------------
@pytest.fixture(scope="session")
def config_manager():
    print("🛠️ Config Manager setup")
    return CONFIG

# ----------------------------------------
# 2️⃣ WebDriver
# ----------------------------------------
@pytest.fixture(scope="function")
def driver(config_manager):
    print("🌐 WebDriver setup")
    driver = get_webdriver(config_manager)
    yield driver
    print("🧹 WebDriver teardown")
    driver.quit()

# ----------------------------------------
# 3️⃣ IMS DB (per test)
# ----------------------------------------

# 3️⃣ IMS DB (Session level) - depends on jvm_manager
@pytest.fixture
def ims(config_manager, request):
    print("💾 IMS setup")
    test_file = Path(request.fspath)
    test_dir_name = test_file.parent.name
    dbd_name = TEST_DBD_MAP.get(test_dir_name)
    if not dbd_name:
        raise ValueError(f"No DBD mapping found for test folder: {test_dir_name}")
    dbd_file = Path(__file__).resolve().parents[0] / "dbd_files" / dbd_name
    if not dbd_file.exists():
        raise FileNotFoundError(f"DBD file not found: {dbd_file}")
    ims_metadata_dir = Path(config_manager.get_section('IMS_DB')['xmlMetadataLocation']) / "DBD"
    ims_metadata_dir.mkdir(parents=True, exist_ok=True)
    dst_file = ims_metadata_dir / "IMSDEV.dbd"
    shutil.copy(str(dbd_file), str(dst_file))
    print(f"Copied DBD for test folder '{test_dir_name}': {dbd_file} → {dst_file}")
    ims_db = IMSDatabase(config_manager, 'IMS_DB')
    ims_db.connect()
    yield ims_db, dbd_file
    print("🧹 IMS teardown")
    ims_db.close()

# ----------------------------------------
# 4️⃣ Target DBs (per session) - params = enabled targets
# ----------------------------------------
@pytest.fixture(scope="function", params=ENABLED_TARGETS)
def target_db(request):
    db_type = request.param
    print(f"🛢️ Setting up target DB: {db_type}")

    if db_type.lower() == "oracle":
        db = OracleDatabase(CONFIG, 'Oracle_DB_Trg')
    elif db_type.lower() == "sqlserver":
        db = SQLServerDatabase(CONFIG, 'MSSQL_DB_Trg')
    else:
        raise ValueError(f"Unsupported target DB: {db_type}")

    db.connect()
    yield db_type, db
    print(f"🧹 {db_type} teardown")
    db.close()

# ----------------------------------------
# 5️⃣ Page Object Models
# ----------------------------------------
@pytest.fixture(scope="function")
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

# ----------------------------------------
# 6️⃣ Default schemas
# ----------------------------------------
@pytest.fixture(scope="function")
def default_schemas(config_manager):
    print("📦 Acquiring default schemas")
    return config_manager.get_default_schemas()

# ----------------------------------------
# 7️⃣ Reset DB before each test
# ----------------------------------------
@pytest.fixture
def reset_database_env(default_schemas, target_db):
    _, db = target_db
    source_schema, target_schema, control_schema = default_schemas
    print(f"🔁 Resetting environment in {db.__class__.__name__}")

    db.drop_all_tables_in_schema(target_schema)
    db.drop_all_tables_in_schema(control_schema)
    db.drop_schema(target_schema)
    db.drop_schema(control_schema)
    db.create_schema(target_schema)
    db.create_schema(control_schema)

# ----------------------------------------
# 8️⃣ Browser UI
# ----------------------------------------
@pytest.fixture(scope="function")
def setup_browser(driver, replicate_pages):
    print("🌍 Browser UI setup")
    replicate_pages.replicate_actions.open_replicate_software()
    replicate_pages.replicate_actions.set_windows_size()
    driver.implicitly_wait(3)
    replicate_pages.replicate_actions.loader_icon_opening_replicate()

# ----------------------------------------
# 9️⃣ Full test environment
# ----------------------------------------
@pytest.fixture
def ims_test(request, config_manager, ims, target_db, replicate_pages, default_schemas, reset_database_env, setup_browser):
    ims_db, dbd_file = ims
    db_type, db = target_db
    test_dir = Path(request.fspath).parent

    env = SimpleNamespace(
        config=config_manager,
        **replicate_pages.__dict__,
        ims_db=ims_db,
        target_db=db,
        target_type=db_type,
        source_schema=ims_db.section['schema'],
        target_schema=default_schemas[1],
        control_schema=default_schemas[2],
        ims_source_name=None,
        target_name=None,
        task_name=None,
        dbd_file=str(dbd_file),
        test_dir=str(test_dir),
        task_logs_dir=str(test_dir / "task_logs"),
        good_files_dir=str(test_dir / "good_files")
    )
    yield env

    replicate_pages.replicate_actions.delete_task_endpoint(
        env.task_name, env.ims_source_name, env.target_name
    )