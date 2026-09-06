"""
Section 2 (Data Types) - ALLTYPES.cpy core numeric/binary/float/packed/zoned matrix.

Loads 20 records covering happy-path, all-zero, min/max boundary, tiny/huge
float magnitude, blank/short/full-length NAME, and elementary REDEFINES
(ALLTYPES-ID / ALLTYPES-ALL-ID) into TZACHA.KSDS.ALLTYPES via vsam_util, then
runs a single Full Load and verifies the entire type matrix against one
.good file - consolidating what would otherwise be ~13 separate test-plan
rows (DT-1 through DT-13) into one automation test, per the agreed
"many records, one .good" consolidation approach already proven in the IMS
suite's array_no_dep_test.py.
"""

from settings import *


def test_alltypes_matrix(vsam_test):
    records = load_json_data(vsam_test, "alltypes_records.json")

    # Load all 20 records into the dataset. clear_first=True gives every run
    # a deterministic starting point regardless of what a prior test left behind.
    vsam_test.vsam_db.load_records(
        dataset="TZACHA.KSDS.ALLTYPES",
        copybook_local_path=vsam_test.copybook_path,
        records=records,
        clear_first=True,
    )

    # Independent readback verification before even touching Replicate - per
    # RP-14811, vsam_util's own exit status is not sufficient proof of success.
    readback = vsam_test.vsam_db.read_records(
        dataset="TZACHA.KSDS.ALLTYPES",
        copybook_local_path=vsam_test.copybook_path,
    )
    assert len(readback) == len(records), (
        f"Expected {len(records)} records after load, found {len(readback)} on readback"
    )

    create_task(vsam_test)

    vsam_test.designer_page.run_new_task()
    vsam_test.monitor_page.wait_for_fl('1')

    collect_logs(vsam_test)
    finalize_test(vsam_test, csv_name="ALLTYPES_MATRIX")