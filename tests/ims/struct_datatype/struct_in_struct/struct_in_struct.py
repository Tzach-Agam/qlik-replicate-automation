from settings import *

def test_struct_in_struct(ims_test):
    """Test for IMS STRUCT inside a STRUCT"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT4\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY1', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY2', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY3', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY4', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY5', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY6', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.connection.commit()
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('1')
    ims_test.monitor_page.cdc_tab()
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY7', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY8', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY9', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY10', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY11', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY12', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")

    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT4\" SET S1_NUM_COL = 99, S1_CHAR_COL = 'UPDAT' WHERE SKEY = 'KEY1'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT4\" SET S2_NUM_COL = 99, S2_CHAR_COL = 'UPDAT' WHERE SKEY = 'KEY2'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT4\" WHERE SKEY = 'KEY3'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT4\" WHERE SKEY = 'KEY4'")
    ims_test.ims_db.connection.commit()
    ims_test.ims_db.sync_command()
    ims_test.monitor_page.insert_check('6')
    ims_test.monitor_page.update_check('2')
    ims_test.monitor_page.delete_check('2')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.source_tasklog_path(), ims_test.task_logs_dir,f"reptask_{ims_test.task_name}.log", ims_test.config)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_STRUCT_IN_STRUCT.csv")
    # compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_STRUCT_IN_STRUCT.good",
    #               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_STRUCT_IN_STRUCT.csv")