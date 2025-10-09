from settings import *

def test_array_without_segment(ims_test):
    """Replication of an array without its base segment"""
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
    ims_test.designer_page.enter_monitor_page()
    ims_test.monitor_page.cdc_tab()
    ims_test.monitor_page.wait_for_message_in_ui("encountered a fatal error")
    ims_test.monitor_page.wait_for_message_in_ui("Failed to allocate Hierarchy Manager.")
    ims_test.monitor_page.wait_for_message_in_ui("Couldn't find table 'public'.'STRUCT2' in capture list")

    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions,
                            ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_NO_SEGMENT.csv")

    log_finder(ims_test.task_logs_dir + f"\\reptask_{ims_test.task_name}.log",
               "encountered a fatal error", ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_NO_SEGMENT.csv")
    log_finder(ims_test.task_logs_dir + f"\\reptask_{ims_test.task_name}.log",
               "Couldn't find table 'public'.'STRUCT2' in capture list",
               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_NO_SEGMENT.csv")

    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_NO_SEGMENT.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_NO_SEGMENT.csv")
