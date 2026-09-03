"""Standalone script to insert rows into the IMS STRUCT3 table.

Mirrors the row-insertion portion of the array_in_array_depon test but does
not depend on the pytest fixtures - it builds the IMS connection directly
from the project's config.ini, the same way the `ims` fixture in
tests/ims/ims_setup_env.py does.

It inserts 1000 rows per bulk in two different bulks (2000 rows total),
generating a unique SKEY per row.
"""

import shutil
from pathlib import Path

from configurations.config_manager import ConfigurationManager
from databases.ims_db import IMSDatabase
from utilities.ims_dbd_mapping import TEST_DBD_MAP


# Full STRUCT3 column list (matches the array_in_array_depon test inserts).
STRUCT3_COLUMNS = (
    "ROOT_ROOTID, SKEY, STU_ID, STU_FIRST_NAME, STU_LAST_NAME, DATE_OF_BIRTH, NUMOF_COURSES, "
    "COURSE_1_COURSE_ID, COURSE_1_COURSE_TITLE, COURSE_1_INSTRUCTOR_ID, COURSE_1_NUMOF_ASSIGNMENTS, "
    "COURSE_1_ASSIGNMENTS_1_ASSIGNMENT_TYPE, COURSE_1_ASSIGNMENTS_1_ASSIGNMENT_TITLE, COURSE_1_ASSIGNMENTS_1_DUE_DATE, COURSE_1_ASSIGNMENTS_1_GRADE, "
    "COURSE_1_ASSIGNMENTS_2_ASSIGNMENT_TYPE, COURSE_1_ASSIGNMENTS_2_ASSIGNMENT_TITLE, COURSE_1_ASSIGNMENTS_2_DUE_DATE, COURSE_1_ASSIGNMENTS_2_GRADE, "
    "COURSE_2_COURSE_ID, COURSE_2_COURSE_TITLE, COURSE_2_INSTRUCTOR_ID, COURSE_2_NUMOF_ASSIGNMENTS, "
    "COURSE_2_ASSIGNMENTS_1_ASSIGNMENT_TYPE, COURSE_2_ASSIGNMENTS_1_ASSIGNMENT_TITLE, COURSE_2_ASSIGNMENTS_1_DUE_DATE, COURSE_2_ASSIGNMENTS_1_GRADE, "
    "COURSE_2_ASSIGNMENTS_2_ASSIGNMENT_TYPE, COURSE_2_ASSIGNMENTS_2_ASSIGNMENT_TITLE, COURSE_2_ASSIGNMENTS_2_DUE_DATE, COURSE_2_ASSIGNMENTS_2_GRADE, "
    "COURSE_3_COURSE_ID, COURSE_3_COURSE_TITLE, COURSE_3_INSTRUCTOR_ID, COURSE_3_NUMOF_ASSIGNMENTS, "
    "COURSE_3_ASSIGNMENTS_1_ASSIGNMENT_TYPE, COURSE_3_ASSIGNMENTS_1_ASSIGNMENT_TITLE, COURSE_3_ASSIGNMENTS_1_DUE_DATE, COURSE_3_ASSIGNMENTS_1_GRADE, "
    "COURSE_3_ASSIGNMENTS_2_ASSIGNMENT_TYPE, COURSE_3_ASSIGNMENTS_2_ASSIGNMENT_TITLE, COURSE_3_ASSIGNMENTS_2_DUE_DATE, COURSE_3_ASSIGNMENTS_2_GRADE, "
    "NUMOF_SUBS, SUB_1_SUB_TYPE, SUB_1_BOOK_NAME, SUB_1_ISBN, SUB_1_RETURN_DATE, SUB_1_JOURNAL_NAME, SUB_1_END_DATE, "
    "SUB_2_SUB_TYPE, SUB_2_BOOK_NAME, SUB_2_ISBN, SUB_2_RETURN_DATE, SUB_2_JOURNAL_NAME, SUB_2_END_DATE"
)

# VALUES tail (everything after SKEY) for each bulk - taken from the test rows.
# {skey} is substituted per row; the leading ROOT_ROOTID is fixed.
BULK_1_VALUES = (
    "'ROOT000003', '{skey}', 10012345, 'JOHN', 'SMITH', 19950515, 2, "
    "50001001, 'INTRODUCTION TO COBOL', 99998888, 2, "
    "'QUIZ', 'DATA TYPES 101', 20231010, 9.5, 'EXAM', 'COBOL BASICS', 20231110, 8.0, "
    "50001002, 'ADVANCED COBOL', 99997777, 1, "
    "'EXAM', 'FILE HANDLING', 20231210, 7.5, '            ', '                                                ', 20231210, 0.0, "
    "0, '                                                ', 0, 0, "
    "'            ', '                                                ', 0, 0.0, '            ', '                                                ', 0, 0.0, "
    "1, 'B', 'THE COBOL BIBLE', '1234567890', 20231231, '                                ', 0, "
    "' ', '                                ', '          ', 0, '                                ', 0"
)

BULK_2_VALUES = (
    "'ROOT000003', '{skey}', 20029876, 'JANE', 'DOE', 19981122, 3, "
    "50002001, 'DATABASE SYSTEMS', 88881111, 2, "
    "'QUIZ', 'SQL BASICS', 20231015, 8.5, 'PROJECT', 'ER DIAGRAMS', 20231115, 9.0, "
    "50002002, 'DATA STRUCTURES', 88882222, 2, "
    "'EXAM', 'LINKED LISTS', 20231020, 7.0, 'EXAM', 'BINARY TREES', 20231120, 6.5, "
    "50002003, 'ALGORITHMS', 88883333, 1, "
    "'PROJECT', 'SORTING ALGORITHMS', 20231205, 9.5, '            ', '                                                ', 0, 0.0, "
    "1, 'J', '                                ', '          ', 0, 'IEEE SOFTWARE', 20240630, "
    "' ', '                                ', '          ', 0, '                                ', 0"
)


def main():
    project_root = Path(__file__).resolve().parents[4]
    config_path = project_root / "configurations" / "config.ini"
    config_manager = ConfigurationManager(str(config_path))

    dbd_name = TEST_DBD_MAP["array_in_array_depon"]
    dbd_file = project_root / "tests" / "ims" / "dbd_files" / dbd_name
    if not dbd_file.exists():
        raise FileNotFoundError(f"DBD file not found: {dbd_file}")

    ims_metadata_dir = Path(config_manager.get_section('IMS_DB')['xmlMetadataLocation']) / "DBD"
    ims_metadata_dir.mkdir(parents=True, exist_ok=True)
    dst_file = ims_metadata_dir / "IMSDEV.dbd"
    shutil.copy(str(dbd_file), str(dst_file))
    print(f"Copied DBD: {dbd_file} → {dst_file}")

    ims_db = IMSDatabase(config_manager, 'IMS_DB')
    ims_db.connect()

    try:
        ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT3\"")
        ims_db.connection.commit()
        print("Cleared STRUCT3 table")

        for i in range(1, 50):
            skey = f"SKEY{i:016d}"
            ims_db.cursor.execute(
                f"INSERT INTO \"DVPCB\".\"STRUCT3\" ({STRUCT3_COLUMNS}) "
                f"VALUES ({BULK_1_VALUES.format(skey=skey)})"
            )
        ims_db.connection.commit()
        print("Finished with first bulk (50 rows)")

        for i in range(50, 100):
            skey = f"SKEY{i:016d}"
            ims_db.cursor.execute(
                f"INSERT INTO \"DVPCB\".\"STRUCT3\" ({STRUCT3_COLUMNS}) "
                f"VALUES ({BULK_2_VALUES.format(skey=skey)})"
            )
        ims_db.connection.commit()
        print("Finished with second bulk (50 rows) - 100 total inserted")

        # Update a subset of the inserted rows, keyed by their unique SKEY.
        # Besides the top-level scalars, modify existing array elements that are
        # populated in both bulks (COURSE_1 + its first assignment, and SUB_1).
        for i in range(1, 100):
            skey = f"SKEY{i:016d}"
            ims_db.cursor.execute(
                "UPDATE \"DVPCB\".\"STRUCT3\" "
                "SET STU_FIRST_NAME = 'UPDATED', STU_LAST_NAME = 'ROW', "
                "COURSE_1_COURSE_TITLE = 'UPDATED COURSE', "
                "COURSE_1_ASSIGNMENTS_1_ASSIGNMENT_TITLE = 'UPDATED ASSIGNMENT', "
                "COURSE_1_ASSIGNMENTS_1_GRADE = 9.9, "
                "SUB_1_BOOK_NAME = 'UPDATED BOOK' "
                f"WHERE SKEY = '{skey}'"
            )
        ims_db.connection.commit()
        print("Finished updating rows")

        # Force IMS to flush the changes, using the sync_command defined in ims_db.py.
        ims_db.sync_command()
    finally:
        ims_db.close()


if __name__ == "__main__":
    main()