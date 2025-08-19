from settings import *

def test_char_datatype(ims_test):
    """Test for IMS Char Data Type"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"ALLTYPES\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 1, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 2, '!@#$%^&*()_-+=[]<>?.,123456', '!@#$%^&*()_-+=[]<>?.,123456')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 3, 'a', 'a')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 4, '', '')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 5, ' ', ' ')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID) VALUES ('ROOT000000', 6)")
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.source_tasklog_path(), ims_test.task_logs_dir,f"reptask_{ims_test.task_name}.log", ims_test.config)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Char.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Char.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Char.csv")