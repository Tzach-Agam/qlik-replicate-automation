import faulthandler
import json

faulthandler.disable()

import pytest
from pathlib import Path
from types import SimpleNamespace
from configurations.config_manager import ConfigurationManager
from browsers.browsers import get_webdriver
from databases.vsam_db import VSAMDatabase
from databases.oracle_db import OracleDatabase
from databases.sqlserver_db import SQLServerDatabase
from replicate_pages import *
from utilities.vsam_metadata_mapping import TEST_METADATA_MAP
from utilities.utility_functions import move_file_to_target_dir, compare_files, log_finder

# ----------------------------------------
# 0 Read config once
# ----------------------------------------
CONFIG = ConfigurationManager(str(Path(__file__).resolve().parents[2] / "configurations" / "config.ini"))
ENABLED_TARGETS = CONFIG.get_enabled_targets()

# ----------------------------------------
# 1 Config Manager
# ----------------------------------------
@pytest.fixture(scope="function")
def config_manager():
    print("Config Manager setup")
    return CONFIG

# ----------------------------------------
# 2 WebDriver
# ----------------------------------------
@pytest.fixture(scope="function")
def driver(config_manager):
    print("WebDriver setup")
    driver = get_webdriver(config_manager)
    yield driver
    print("WebDriver teardown")
    driver.quit()

# ----------------------------------------
# 3 VSAM DB (per test)
# ----------------------------------------
@pytest.fixture
def vsam(config_manager, request):
    print("VSAM setup")
    test_file = Path(request.fspath)
    test_dir_name = test_file.parent.name
    zip_name = TEST_METADATA_MAP.get(test_dir_name)
    if not zip_name:
        raise ValueError(f"No metadata zip mapping found for test folder: {test_dir_name}")
    zip_file = Path(__file__).resolve().parents[0] / "metadata_zips" / zip_name
    if not zip_file.exists():
        raise FileNotFoundError(f"Metadata zip not found: {zip_file}")
    print(f"Resolved metadata zip for test folder '{test_dir_name}': {zip_file}")
    vsam_db = VSAMDatabase(config_manager, 'VSAM_DB')
    vsam_db.connect()
    yield vsam_db, zip_file
    print("VSAM teardown")
    vsam_db.close()

# ----------------------------------------
# 4 Target DBs (per session) - params = enabled targets
# ----------------------------------------
@pytest.fixture(scope="function", params=ENABLED_TARGETS)
def target_db(request):
    db_type = request.param
    print(f"Setting up target DB: {db_type}")

    if db_type.lower() == "oracle":
        db = OracleDatabase(CONFIG, 'Oracle_DB_Trg')
    elif db_type.lower() == "sqlserver":
        db = SQLServerDatabase(CONFIG, 'MSSQL_DB_Trg')
    else:
        raise ValueError(f"Unsupported target DB: {db_type}")

    db.connect()
    yield db_type, db
    print(f"{db_type} teardown")
    db.close()

# ----------------------------------------
# 5 Page Object Models
# ----------------------------------------
@pytest.fixture(scope="function")
def replicate_pages(driver, config_manager):
    print("Replicate Pages setup")
    return SimpleNamespace(
        replicate_actions=ReplicateCommonActions(driver, config_manager),
        tasks_general_page=TasksPage(driver),
        new_task_page=NewTaskPage(driver),
        manage_endpoints=ManageEndpoints(driver, config_manager),
        task_settings=TaskSettings(driver, config_manager),
        table_selection=TableSelection(driver, config_manager),
        designer_page=DesignerPage(driver),
        monitor_page=MonitorPage(driver),
        table_settings=TableSettings(driver)
    )

# ----------------------------------------
# 6 Default schemas
# ----------------------------------------
@pytest.fixture(scope="function")
def default_schemas(config_manager):
    print("Acquiring default schemas")
    return config_manager.get_default_schemas()

# ----------------------------------------
# 7 Reset DB before each test
# ----------------------------------------
@pytest.fixture(scope="function")
def reset_database_env(default_schemas, target_db):
    _, db = target_db
    source_schema, target_schema, control_schema = default_schemas
    print(f"Resetting environment in {db.__class__.__name__}")

    db.drop_all_tables_in_schema(target_schema)
    db.drop_all_tables_in_schema(control_schema)
    db.drop_schema(target_schema)
    db.drop_schema(control_schema)
    db.create_schema(target_schema)
    db.create_schema(control_schema)

# ----------------------------------------
# 8 Browser UI
# ----------------------------------------
@pytest.fixture(scope="function")
def setup_browser(driver, replicate_pages):
    print("Browser UI setup")
    replicate_pages.replicate_actions.open_replicate_software()
    replicate_pages.replicate_actions.set_windows_size()
    driver.implicitly_wait(3)
    replicate_pages.replicate_actions.loader_icon_opening_replicate()

# ----------------------------------------
# Helpers - copybook auto-discovery and named JSON data loading
# ----------------------------------------
def _discover_copybook(test_dir_path):
    """Every test's copybook lives at <test_dir>/data/<name>.cpy - exactly one per test.
    Test files never need to name it explicitly."""
    data_dir = Path(test_dir_path) / "data"
    cpy_files = sorted(data_dir.glob("*.cpy"))
    if not cpy_files:
        raise FileNotFoundError(f"No .cpy file found in {data_dir}")
    if len(cpy_files) > 1:
        raise ValueError(f"Expected exactly one .cpy file in {data_dir}, found {len(cpy_files)}: {cpy_files}")
    return cpy_files[0]


def load_json_data(vsam_test, filename):
    """Load a named JSON data file from this test's data/ folder. The test itself names
    the file (not settings.py) so a test can later load several data files independently -
    e.g. one for Full Load seed data, another for CDC insert/update/delete data."""
    with open(Path(vsam_test.test_dir_path) / "data" / filename, "r", encoding="utf-8") as f:
        return json.load(f)


# ----------------------------------------
# 9 Full test environment
# ----------------------------------------
@pytest.fixture(scope="function")
def vsam_test(request, config_manager, vsam, target_db, replicate_pages, default_schemas, reset_database_env, setup_browser):
    vsam_db, zip_file = vsam
    db_type, db = target_db
    test_dir_path = Path(request.fspath).parent
    test_dir_name = test_dir_path.name

    env = SimpleNamespace(
        config=config_manager,
        **replicate_pages.__dict__,
        vsam_db=vsam_db,
        target_db=db,
        target_type=db_type,
        target_schema=default_schemas[1],
        control_schema=default_schemas[2],
        vsam_source_name=None,
        target_name=None,
        task_name=None,
        zip_metadata_file=str(zip_file),
        copybook_path=str(_discover_copybook(test_dir_path)),
        test_dir_path=str(test_dir_path),
        test_dir_name=str(test_dir_name),
        task_logs_dir=str(test_dir_path / "task_logs"),
        good_files_dir=str(test_dir_path / "good_files")
    )

    yield env

    if env.task_name:
        replicate_pages.replicate_actions.delete_task_endpoint(
            env.task_name, env.vsam_source_name, env.target_name)
    else:
        print("Skipping task deletion - task was never created", flush=True)


def collect_logs(vsam_test):
    vsam_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(vsam_test.config.replicate_logs_path(), vsam_test.task_logs_dir,
                             f"reptask_{vsam_test.task_name}.log", vsam_test.config,
                             vsam_test.replicate_actions, vsam_test.task_name)


def finalize_test(vsam_test, csv_name, log_checks=None):
    """
    :param log_checks: optional list of (log_file_path, search_text) tuples. When given, log_finder
    appends its result line for each tuple, in order, to the exported CSV after the schema export
    and before the comparison, since export_schema_data_to_csv overwrites the file and would
    otherwise wipe out an earlier log_finder call.
    """
    csv_path = vsam_test.good_files_dir + f"\\{csv_name}.csv"
    vsam_test.target_db.export_schema_data_to_csv(vsam_test.target_schema, csv_path)
    for log_file_path, search_text in log_checks or []:
        log_finder(log_file_path, search_text, csv_path)
    compare_files(vsam_test.good_files_dir + f"\\{csv_name}.good", csv_path)