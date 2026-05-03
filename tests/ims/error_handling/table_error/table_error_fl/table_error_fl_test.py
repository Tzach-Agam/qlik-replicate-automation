from settings import *

def test_table_error_fl(ims_test):
    """Test for Table error in IMS in FL process"""
    create_task(ims_test)

    if ims_test.target_type == "oracle":
        ims_test.target_db.execute_query(
            f"CREATE TABLE \"{ims_test.target_schema}\".STRUCT3__ARRAY_BASE__ARRAY_1__ARRAY_2 (ROOTID NVARCHAR2(20) NOT NULL, SKEY NVARCHAR2(40) NOT NULL, ARRAY_BASE_ROWNUM NUMBER(10) NOT NULL, ARRAY_1_ROWNUM NUMBER(10) NOT NULL, ARRAY_2_ROWNUM NUMBER(10) NOT NULL, CHAR_COL_2 NVARCHAR2(10), DECIMAL_COL_2 NUMBER NOT NULL, PRIMARY KEY (ROOTID,SKEY,ARRAY_BASE_ROWNUM,ARRAY_1_ROWNUM,ARRAY_2_ROWNUM))")
    if ims_test.target_type == "sqlserver":
        ims_test.target_db.execute_query(
            f"CREATE TABLE \"{ims_test.target_schema}\".STRUCT3__ARRAY_BASE__ARRAY_1__ARRAY_2 (ROOTID nvarchar(20) NOT NULL,	SKEY nvarchar(40) NOT NULL,	ARRAY_BASE_ROWNUM int NOT NULL,	ARRAY_1_ROWNUM int NOT NULL, ARRAY_2_ROWNUM int NOT NULL, CHAR_COL_2 nvarchar(10), DECIMAL_COL_2 NUMERIC NOT NULL, PRIMARY KEY (ARRAY_1_ROWNUM,ARRAY_2_ROWNUM,ARRAY_BASE_ROWNUM,ROOTID,SKEY))")

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT3\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_CHAR_COL_4, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_ARRAY_5_1_CHAR_COL_5)"
        "VALUES ('ROOT000003', 'KEY1', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD', 'EEEEE', 'FFFFF')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_CHAR_COL_4, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_ARRAY_5_1_CHAR_COL_5)"
        "VALUES ('ROOT000003', 'KEY2', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD', 'EEEEE', 'FFFFF')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_CHAR_COL_4, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_ARRAY_5_1_CHAR_COL_5)"
        "VALUES ('ROOT000003', 'KEY3', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD', 'EEEEE', 'FFFFF')")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.designer_page.enter_monitor_page()
    ims_test.monitor_page.cdc_tab()
    ims_test.monitor_page.wait_for_message_in_ui("Table 'IMSDEV'.'STRUCT3' (subtask 1 thread 1) is suspended")
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')

    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions,
                            ims_test.task_name)

    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}TABLE_ERROR_FL.csv")

    log_finder(ims_test.task_logs_dir + f"\\reptask_{ims_test.task_name}.log",
               "]W:  Table 'IMSDEV'.'STRUCT3' (subtask 1 thread 1) is suspended",
               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}TABLE_ERROR_FL.csv")
    log_finder(ims_test.task_logs_dir + f"\\reptask_{ims_test.task_name}.log",
               "]E:  Handling End of table 'replicate_selenium_target'.'STRUCT3__ARRAY_BASE__ARRAY_1__ARRAY_2' loading failed",
               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}TABLE_ERROR_FL.csv")

    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}TABLE_ERROR_FL.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}TABLE_ERROR_FL.csv")