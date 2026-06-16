from settings import *

def test_fl_cdc(ims_test):
    """Replication of invalid segments in DBD file"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT2\"")
    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT3\"")
    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT4\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY1', 'Full_Load', 1111, 'Full_Load', 1111, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY2', 'Full_Load', 2222, 'Full_Load', 2222, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY3', 'Full_Load', 3333, 'Full_Load', 3333, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY1', 'Full_Load', 1111, 'Full_Load', 1111, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY2', 'Full_Load', 2222, 'Full_Load', 2222, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY3', 'Full_Load', 3333, 'Full_Load', 3333, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY1', 'Full_Load', 1111, 'Full_Load', 1111, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY2', 'Full_Load', 2222, 'Full_Load', 2222, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY3', 'Full_Load', 3333, 'Full_Load', 3333, 'Full_Load')")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY4', 'Capture_Changes', 1111, 'Capture_Changes', 1111, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY5', 'Capture_Changes', 2222, 'Capture_Changes', 2222, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY6', 'Capture_Changes', 3333, 'Capture_Changes', 3333, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY4', 'Capture_Changes', 1111, 'Capture_Changes', 1111, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY5', 'Capture_Changes', 2222, 'Capture_Changes', 2222, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY6', 'Capture_Changes', 3333, 'Capture_Changes', 3333, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY4', 'Capture_Changes', 1111, 'Capture_Changes', 1111, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY5', 'Capture_Changes', 2222, 'Capture_Changes', 2222, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY6', 'Capture_Changes', 3333, 'Capture_Changes', 3333, 'Capture_Changes')")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('3', '3')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    collect_logs(ims_test)
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config,
                            ims_test.replicate_actions, ims_test.task_name)
    log_finder(ims_test.java + f"\\reptask_{ims_test.task_name}.log",
               "Source changes that would have had no impact were not applied to the target database",
               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_AC_INSERT.csv")
    finalize_test(ims_test, f"IMS_2_{ims_test.target_db.config['endpoint']}_INVALID_SEGMENTS.csv")