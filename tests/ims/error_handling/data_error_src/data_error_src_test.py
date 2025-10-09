from settings import *

def test_data_error_src(ims_test):
    """Test for Data error in IMS from source side"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT2\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY1', 'Full_Load', 1111, 'Full_Load', 1111, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY2', 'Full_Load', 2222, 'Full_Load', 2222, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY3', 'Full_Load', 3333, 'Full_Load', 3333, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY4', 'Full_Load', 4444, 'Full_Load', 4444, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY5', 'Full_Load', 5555, 'Full_Load', 5555, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY6', 'Full_Load', 'Full_Load', 1111, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY7', 'Full_Load', 2222, 'Full_Load', 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY8', 'Full_Load', 'Full_Load', 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY)"
        "VALUES('ROOT000002', 'KEY9')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY)"
        "VALUES('ROOT000002', 'KEY10')")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()

    ims_test.monitor_page.wait_for_message_in_ui(
        "Unloading of segment 'STRUCT2' finished with 4 data errors")
    ims_test.monitor_page.wait_for_message_in_ui(
        "Error converting field 'COL2_DECIMAL', error message: 'An error occurred converting the field COL2_DECIMAL in the struct STRUCT2.BASIC_TABLE")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY11', 'Full_Load', 1111, 'Full_Load', 1111, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY12', 'Full_Load', 2222, 'Full_Load', 2222, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY13', 'Full_Load', 3333, 'Full_Load', 3333, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY14', 'Full_Load', 4444, 'Full_Load', 4444, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY15', 'Full_Load', 5555, 'Full_Load', 5555, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL4_DECIMAL, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY16', 'Full_Load', 'Full_Load', 1111, 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL2_DECIMAL, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY17', 'Full_Load', 2222, 'Full_Load', 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, BASIC_TABLE_1_COL1_CHAR, BASIC_TABLE_1_COL3_CHAR, BASIC_TABLE_1_COL5_CHAR)"
        "VALUES('ROOT000002', 'KEY18', 'Full_Load', 'Full_Load', 'Full_Load')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY)"
        "VALUES('ROOT000002', 'KEY19')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY)"
        "VALUES('ROOT000002', 'KEY20')")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.insert_check('8', '10')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.wait_for_message_in_ui(
        "Error converting field 'COL2_DECIMAL', error message: 'An error occurred converting the field COL2_DECIMAL in the struct STRUCT2.BASIC_TABLE")
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir, f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions, ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}DATA_ERROR_SRC.csv")

    log_finder(ims_test.task_logs_dir + f"\\reptask_{ims_test.task_name}.log",
               "]W:  Unloading of segment 'STRUCT2' finished with 4 data errors",
               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}DATA_ERROR_SRC.csv")

    log_finder(ims_test.task_logs_dir + f"\\reptask_{ims_test.task_name}.log",
               "]W:  Error converting field 'COL2_DECIMAL', error message: 'An error occurred converting the field COL2_DECIMAL in the struct STRUCT2.BASIC_TABLE",
               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}DATA_ERROR_SRC.csv")


    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}DATA_ERROR_SRC.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}DATA_ERROR_SRC.csv")