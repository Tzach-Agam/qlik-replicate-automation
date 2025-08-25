from settings import *

def test_array_in_array(ims_test):
    """Test for IMS ARRAY inside an ARRAY"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT3\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY1', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY2', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY3', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY4', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY5', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY6', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY)"
                                   "VALUES ('ROOT000003', 'KEY7')")
    ims_test.ims_db.connection.commit()
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('4')
    ims_test.monitor_page.cdc_tab()
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY8', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY9', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY10', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY11', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY12', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY13', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY)"
                                   "VALUES ('ROOT000003', 'KEY14')")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT3\" SET ARRAY_BASE_1_CHAR_COL_BASE = 'UPDAT', ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1 = 'UPDAT', ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2 = 'UPDAT', ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3 = 'UPDAT' WHERE SKEY = 'KEY2'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT3\" SET ARRAY_BASE_1_CHAR_COL_BASE = 'UPDAT', ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1 = 'UPDAT', ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2 = 'UPDAT', ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3 = 'UPDAT' WHERE SKEY = 'KEY3'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT3\" WHERE SKEY = 'KEY1'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT3\" WHERE SKEY = 'KEY7'")
    ims_test.ims_db.connection.commit()
    ims_test.ims_db.sync_command()
    ims_test.monitor_page.insert_check('7', '7', '7', '7')
    ims_test.monitor_page.update_check('2', '2', '2', '0')
    ims_test.monitor_page.delete_check('2', '2', '2', '2')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.source_tasklog_path(), ims_test.task_logs_dir,f"reptask_{ims_test.task_name}.log", ims_test.config)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_IN_ARRAY.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_IN_ARRAY.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_IN_ARRAY.csv")