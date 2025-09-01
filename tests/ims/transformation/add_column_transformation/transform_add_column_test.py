from settings import *

def test_transform_add_column(ims_test):
    """Test for adding a column in transformation"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT2\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY1', 11, 'AAAAA', 11, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY2', 22, 'AAAAA', 22, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY3', 33, 'AAAAA', 33, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY4', 44, 'AAAAA', 44, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY5', 55, 'AAAAA', 55, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY6', 66, 'AAAAA', 66, 'AAAAA')")
    ims_test.ims_db.connection.commit()
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY7', 77, 'AAAAA', 11, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY8', 88, 'AAAAA', 88, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY9', 99, 'AAAAA', 99, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY10', 10, 'AAAAA', 10, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY11', 11, 'AAAAA', 11, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, TRANSFORM_TABLE_1_COL_NUM1, TRANSFORM_TABLE_1_COL_CHAR1, TRANSFORM_TABLE_1_COL_NUM2, TRANSFORM_TABLE_1_COL_CHAR2)"
        "VALUES ('ROOT000002', 'KEY12', 12, 'AAAAA', 12, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\" SET TRANSFORM_TABLE_1_COL_NUM1 = 99 TRANSFORM_TABLE_1_COL_CHAR1 = 'UPDAT' WHERE SKEY = 'KEY1'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\" SET TRANSFORM_TABLE_1_COL_NUM2 = 99 TRANSFORM_TABLE_1_COL_CHAR2 = 'UPDAT' WHERE SKEY = 'KEY2'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT2\" WHERE SKEY = 'KEY3'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT2\" WHERE SKEY = 'KEY4'")
    ims_test.ims_db.connection.commit()
    ims_test.ims_db.sync_command()
    ims_test.monitor_page.insert_check('6')
    ims_test.monitor_page.update_check('2')
    ims_test.monitor_page.delete_check('2')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.source_tasklog_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}TRANS_EXPRESSION.csv")
    # compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_DATATYPES.good",
    #               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_DATATYPES.csv")
