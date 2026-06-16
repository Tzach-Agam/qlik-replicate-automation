# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A Selenium + pytest automation suite that drives the **Qlik Replicate** on-premise web UI to test end-to-end data replication. A test creates source/target endpoints and a replication task through the browser, mutates data directly in a source database, lets Replicate replicate it, then exports the target schema to CSV and diffs it against a checked-in `.good` file. The diff *is* the assertion.

These tests are **not hermetic**: they require a running Replicate server, live source/target databases, JDBC drivers, and a configured `config.ini`. They cannot run in CI without that whole environment. Currently only the **IMS source** suite (`tests/ims/`) is implemented despite the README/`description.txt` describing more.

## Commands

```bash
pip install -r requirements.txt          # Python 3.x; needs a JVM on PATH for IMS (jpype/jaydebeapi)

pytest                                    # run everything under tests/ (testpaths in pytest.ini)
pytest -m ims                             # run by marker (ims, snowflake, mongodb, slow, api, regression)
pytest tests/ims/datatypes_ims/char_table # run a single test directory
pytest tests/ims/datatypes_ims/char_table/char_table_test.py::test_char_table   # single test
```

`pytest.ini` sets `addopts = -s -v --dist loadfile` (pytest-xdist `loadfile` keeps one file's tests on one worker) and `pythonpath = .` so `from configurations...`, `from databases...` etc. resolve. Each test file also does `from settings import *`, which works because pytest's rootdir insertion puts the test's own directory on `sys.path`.

## Configuration (required to run anything)

`configurations/config.ini` is **gitignored** and contains live hosts and credentials — never commit it or copy its secrets into other files. `ConfigurationManager` (`configurations/config_manager.py`) is the single accessor for all settings; read config through its methods rather than parsing the ini elsewhere.

Key behaviors:
- **`[OS] Linux = true/false` is the master switch.** Almost every path/URL/credential getter (`get_base_url`, `get_username`, `get_password`, `replicate_logs_path`, etc.) picks a `*_lin` or `*_win` key based on it. Flip one flag to retarget the whole environment between the Windows and Linux Replicate servers.
- **`[Targets]`** lists target endpoints with `true`/`false`. `get_enabled_targets()` returns the enabled ones, and the `target_db` fixture is **parametrized** over that list — enabling Oracle *and* SQLServer runs every test once per target.
- Each DB section (e.g. `[Oracle_DB_Trg]`, `[MSSQL_DB_Trg]`) carries an `endpoint` name (used in `.good`/CSV filenames) and a `create_endpoint_method` name that is resolved dynamically via `getattr` on `ManageEndpoints` — see `settings.py`.

## Architecture

Page Object Model. Four layers, wired together by pytest fixtures:

1. **`browsers/browsers.py`** — `get_webdriver(config)` factory returns a Chrome/Edge/Firefox driver based on `[Browser] driver`, configured for the download directory and `[Display_Mode] headless`.
2. **`replicate_pages/`** — one class per Replicate UI screen (`DesignerPage`, `ManageEndpoints`, `MonitorPage`, `TableSelection`, `TaskSettings`, etc.), all re-exported from `replicate_pages/__init__.py`. `ReplicateCommonActions` holds cross-page actions (open app, navigate, download/delete task). These encapsulate all Selenium locators and waits; tests should not touch the driver directly.
3. **`databases/`** — one class per DB engine (`IMSDatabase`, `OracleDatabase`, `SQLServerDatabase`, `MongoDBDatabase`, `SnowflakeDatabase`). Source classes execute DML to generate change data; target classes own `export_schema_data_to_csv` / `drop_all_tables_in_schema` / schema lifecycle. IMS connects over **JDBC via jpype/jaydebeapi** (`imsudb.jar`), unlike the native drivers used elsewhere.
4. **`utilities/`** — `utility_functions.py` (`safe_click`, `move_file_to_target_dir`, `compare_files`, `log_finder`) and `ims_dbd_mapping.py` (`TEST_DBD_MAP`).

### The IMS test environment (`tests/ims/ims_setup_env.py`)

This module is the real conftest — every IMS `settings.py` does `from tests.ims.ims_setup_env import *`. It defines a chain of `function`-scoped fixtures that compose into the top-level **`ims_test`** fixture, a `SimpleNamespace` carrying every page object, both DB connections, schema names, and per-test paths. A test function just takes `ims_test` as its argument.

Fixture composition: `config_manager` → `driver` → (`ims`, `target_db`, `replicate_pages`, `default_schemas`, `reset_database_env`, `setup_browser`) → `ims_test`.

- `CONFIG` is built **once at import time**; `target_db` is parametrized over `get_enabled_targets()`.
- The `ims` fixture maps the **test directory name** → a `.dbd` file via `TEST_DBD_MAP`, copies it into the IMS metadata location as `IMSDEV.dbd`, then connects. Adding an IMS test requires an entry in `TEST_DBD_MAP` keyed by the test's folder name, or it raises `ValueError`.
- `reset_database_env` drops/recreates the target and control schemas before each test.
- Teardown deletes the task/endpoints (only if a task was created).

### Per-test directory layout

```
tests/ims/<group>/<test_name>/
  <test_name>_test.py     # test function taking ims_test
  settings.py             # from ims_setup_env import *; create_task() builds endpoints+task for this scenario
  good_files/             # IMS_2_<endpoint>_<NAME>.good — expected target state per target type
  task_logs/              # Replicate logs pulled in during the run (created at runtime)
```

### Typical test flow

`create_task(ims_test)` (in `settings.py`) builds endpoints + task and selects tables → test runs DML on the IMS source for Full Load → `monitor_page.wait_for_fl(...)` → switches to CDC, runs more DML → `ims_db.sync_command()` forces IMS to flush changes → `monitor_page.wait_for_cdc()` and `insert_check/update_check/delete_check` assert operation counts in the UI → `stop_task()` → `collect_logs(ims_test)` → `finalize_test(ims_test, "IMS_2_<endpoint>_<NAME>")` exports the target schema to CSV and `compare_files` diffs it against the `.good` file (deletes the CSV on match, raises `AssertionError` on mismatch).

When replication logic changes legitimately alter output, regenerate the `.good` files (the repo history shows this is routine — "Changed good files according to..."). The `endpoint` token in the filename comes from the target section's `endpoint` config value, so each enabled target needs its own `.good` file.
