from settings import *

def test_data_error_trg_fl(ims_test):
    """Test for Data error in IMS During FL process"""
    create_task(ims_test)

    if ims_test.target_type == "oracle":
        ims_test.target_db.execute_query(f"CREATE TABLE \"{ims_test.target_schema}\".STRUCT2__BASIC_TABLE (ROOTID NVARCHAR2(20), SKEY NVARCHAR2(40), BASIC_TABLE_ROWNUM NUMBER, COL5_CHAR NVARCHAR2(50), COL4_DECIMAL NUMBER, COL3_CHAR NVARCHAR2(50), COL2_DECIMAL NUMBER NOT NULL, COL1_CHAR NVARCHAR2(50), PRIMARY KEY (ROOTID, SKEY, BASIC_TABLE_ROWNUM))")
    if ims_test.target_type == "sqlserver":
        ims_test.target_db.execute_query(f"CREATE TABLE \"{ims_test.target_schema}\".STRUCT2__BASIC_TABLE (ROOTID NVARCHAR(20), SKEY NVARCHAR(40), BASIC_TABLE_ROWNUM NUMERIC, COL5_CHAR NVARCHAR(50), COL4_DECIMAL NUMERIC, COL3_CHAR NVARCHAR(50), COL2_DECIMAL NUMERIC NOT NULL, COL1_CHAR NVARCHAR(50), PRIMARY KEY (ROOTID, SKEY, BASIC_TABLE_ROWNUM))")

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT2\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY1', 'Full_Load', 'Full_Load', 1111, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY2', 'Full_Load', 'Full_Load', 2222, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY3', 'Full_Load', 'Full_Load', 3333, 'Full_Load')")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.designer_page.enter_monitor_page()
    ims_test.monitor_page.cdc_tab()
    ims_test.monitor_page.wait_for_message_in_ui("Table 'IMSDEV'.'STRUCT2' (subtask 1 thread 1) is suspended")
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir, f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions, ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}DATA_ERROR_FL.csv")
    log_finder(ims_test.task_logs_dir + f"\\reptask_{ims_test.task_name}.log",
               "]W:  Table 'IMSDEV'.'STRUCT2' (subtask 1 thread 1) is suspended",
               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}DATA_ERROR_FL.csv")
    log_finder(ims_test.task_logs_dir + f"\\reptask_{ims_test.task_name}.log",
               "]E:  Handling End of table 'replicate_selenium_target'.'STRUCT2__BASIC_TABLE' loading failed",
               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}DATA_ERROR_FL.csv")

    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}DATA_ERROR_FL.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}DATA_ERROR_FL.csv")