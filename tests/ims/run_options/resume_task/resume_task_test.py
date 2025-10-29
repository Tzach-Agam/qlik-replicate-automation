from settings import *
from time import sleep

def test_resume_task(ims_test):
    """Resume task with IMS source"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT2\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY1', 'Full_Load', 1111, 'Full_Load', 1111, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY2', 'Full_Load', 2222, 'Full_Load', 2222, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY3', 'Full_Load', 3333, 'Full_Load', 3333, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY4', 'Full_Load', 4444, 'Full_Load', 4444, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY5', 'Full_Load', 5555, 'Full_Load', 5555, 'Full_Load')")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY6', 'Capture_Changes', 1111, 'Capture_Changes', 1111, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY7', 'Capture_Changes', 2222, 'Capture_Changes', 2222, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY8', 'Capture_Changes', 3333, 'Capture_Changes', 3333, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY9', 'Capture_Changes', 4444, 'Capture_Changes', 4444, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY10', 'Capture_Changes', 5555, 'Capture_Changes', 5555, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\"  SET BASIC_TABLE_1_COL1_CHAR = 'UPDATE' WHERE SKEY = 'KEY1'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\"  SET BASIC_TABLE_1_COL1_CHAR = 'UPDATE' WHERE SKEY = 'KEY2'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\"  SET BASIC_TABLE_1_COL1_CHAR = 'UPDATE' WHERE SKEY = 'KEY6'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\"  SET BASIC_TABLE_1_COL1_CHAR = 'UPDATE' WHERE SKEY = 'KEY7'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT2\"  WHERE SKEY = 'KEY3'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT2\"  WHERE SKEY = 'KEY8'")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('5', '5')
    ims_test.monitor_page.update_check('4', '0')
    ims_test.monitor_page.delete_check('2', '2')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY11', 'Capture_Changes', 1111, 'Capture_Changes', 1111, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY12', 'Capture_Changes', 2222, 'Capture_Changes', 2222, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY13', 'Capture_Changes', 3333, 'Capture_Changes', 3333, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY14', 'Capture_Changes', 4444, 'Capture_Changes', 4444, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_NUM, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_NUM, BASIC_TABLE_1_COL5_CHAR) "
        "VALUES('ROOT000002', 'KEY15', 'Capture_Changes', 5555, 'Capture_Changes', 5555, 'Capture_Changes')")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\"  SET BASIC_TABLE_1_COL1_CHAR = 'UPDATE' WHERE SKEY = 'KEY9'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT2\"  WHERE SKEY = 'KEY10'")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()
    sleep(5)

    ims_test.monitor_page.resume_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('5', '5')
    ims_test.monitor_page.update_check('1', '0')
    ims_test.monitor_page.delete_check('1', '1')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions, ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_RESUME_TASK.csv")

    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_RESUME_TASK.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_RESUME_TASK.csv")