from settings import *

def test_sub_array_without_parent_array(ims_test):
    """Replication of sub array without parent array"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT3\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY1', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY2', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY3', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY4', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT3\" (ROOT_ROOTID, SKEY, ARRAY_BASE_1_CHAR_COL_BASE, ARRAY_BASE_1_ARRAY_1_1_CHAR_COL_1, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_CHAR_COL_2, ARRAY_BASE_1_ARRAY_1_1_ARRAY_2_1_ARRAY_3_1_CHAR_COL_3)"
        "VALUES ('ROOT000003', 'KEY5', 'AAAAA', 'BBBBB', 'CCCCC', 'DDDDD')")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.designer_page.enter_monitor_page()
    ims_test.monitor_page.cdc_tab()
    ims_test.monitor_page.wait_for_message_in_ui("encountered a fatal error")
    ims_test.monitor_page.wait_for_message_in_ui("Failed to allocate Hierarchy Manager.")
    ims_test.monitor_page.wait_for_message_in_ui("Couldn't find table 'public'.'ARRAY_BASE' in capture list.")

    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions,
                            ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_SUB_ARRAY_NO_PARENT.csv")

    log_finder(ims_test.task_logs_dir + f"\\reptask_{ims_test.task_name}.log",
               "encountered a fatal error", ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_SUB_ARRAY_NO_PARENT.csv")
    log_finder(ims_test.task_logs_dir + f"\\reptask_{ims_test.task_name}.log",
               "Couldn't find table 'public'.'STRUCT2' in capture list", ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_SUB_ARRAY_NO_PARENT.csv")

    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_SUB_ARRAY_NO_PARENT.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_SUB_ARRAY_NO_PARENT.csv")