from settings import *

def test_table_error_cdc(ims_test):
    """Test for Table error in IMS in CDC process"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT3\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_DECIMAL_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_CHAR_COL_4, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_ARRAY_5_1_CHAR_COL_5)"
        "VALUES ('ROOT000003', 'KEY1', 'AAAAA', 'BBBBB', 'CCCCC', 1111, 'DDDDD', 'EEEEE', 'FFFFF')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_DECIMAL_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_CHAR_COL_4, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_ARRAY_5_1_CHAR_COL_5)"
        "VALUES ('ROOT000003', 'KEY2', 'AAAAA', 'BBBBB', 'CCCCC', 2222, 'DDDDD', 'EEEEE', 'FFFFF')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_DECIMAL_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_CHAR_COL_4, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_ARRAY_5_1_CHAR_COL_5)"
        "VALUES ('ROOT000003', 'KEY3', 'AAAAA', 'BBBBB', 'CCCCC', 3333, 'DDDDD', 'EEEEE', 'FFFFF')")
    ims_test.ims_db.connection.commit()
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('6')
    ims_test.monitor_page.cdc_tab()
    ims_test.target_db.execute_query(f"DROP TABLE \"{ims_test.target_schema}\".ARRAY_2")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_DECIMAL_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_CHAR_COL_4, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_ARRAY_5_1_CHAR_COL_5)"
        "VALUES ('ROOT000003', 'KEY4', 'AAAAA', 'BBBBB', 'CCCCC', 4444, 'DDDDD', 'EEEEE', 'FFFFF')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_DECIMAL_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_CHAR_COL_4, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_ARRAY_5_1_CHAR_COL_5)"
        "VALUES ('ROOT000003', 'KEY5', 'AAAAA', 'BBBBB', 'CCCCC', 5555, 'DDDDD', 'EEEEE', 'FFFFF')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_DECIMAL_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_CHAR_COL_4, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_ARRAY_4_1_ARRAY_5_1_CHAR_COL_5)"
        "VALUES ('ROOT000003', 'KEY6', 'AAAAA', 'BBBBB', 'CCCCC', 6666, 'DDDDD', 'EEEEE', 'FFFFF')")
    ims_test.ims_db.connection.commit()
    ims_test.ims_db.sync_command()
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.wait_for_message_in_ui("Failed to apply INSERT")
    ims_test.monitor_page.wait_for_message_in_ui("Table 'public'.'ARRAY_2' (subtask 0 thread 1) is suspended")
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    ims_test.replicate_actions.navigate_to_main_page('tasks')

    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions,
                            ims_test.task_name)

    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}TABLE_ERROR_CDC.csv")

    log_finder(ims_test.task_logs_dir + f"\\reptask_{ims_test.task_name}.log",
               " ]W:  Table 'public'.'ARRAY_2' (subtask 0 thread 1) is suspended",
               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}TABLE_ERROR_CDC.csv")

    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}TABLE_ERROR_CDC.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}TABLE_ERROR_CDC.csv")