from settings import *

def test_time_datatype(ims_test):
    """Test for IMS TIME Data Type"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"ALLTYPES\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, TIME_TABLE_1_TIMESTAMP_COL, TIME_TABLE_1_TIME_COL)"
        "VALUES ('ROOT000000', 1, '2025-14-08 10:30:45', '10:30:45')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, TIME_TABLE_1_TIMESTAMP_COL, TIME_TABLE_1_TIME_COL)"
        "VALUES ('ROOT000000', 2, '1999-31-12 23:59:59', '23:59:59')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, TIME_TABLE_1_TIMESTAMP_COL, TIME_TABLE_1_TIME_COL)"
        "VALUES ('ROOT000000', 3, '2000-01-01 00:00:00', '00:00:00')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, TIME_TABLE_1_TIMESTAMP_COL, TIME_TABLE_1_TIME_COL)"
        "VALUES ('ROOT000000', 4, '1985-15-06 08:05:59', '08:05:59')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, TIME_TABLE_1_TIMESTAMP_COL, TIME_TABLE_1_TIME_COL)"
        "VALUES ('ROOT000000', 5, '2024-29-02 12:30:15', '12:30:15')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID)"
        "VALUES('ROOT000000', 6)")
    ims_test.ims_db.connection.commit()
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, TIME_TABLE_1_TIMESTAMP_COL, TIME_TABLE_1_TIME_COL)"
        "VALUES ('ROOT000000', 7, '1970-01-01 00:00:01', '00:00:01')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, TIME_TABLE_1_TIMESTAMP_COL, TIME_TABLE_1_TIME_COL)"
        "VALUES ('ROOT000000', 8, '2010-25-12 18:45:30', '18:45:30')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, TIME_TABLE_1_TIMESTAMP_COL, TIME_TABLE_1_TIME_COL)"
        "VALUES ('ROOT000000', 9, '1969-31-12 23:59:58', '23:59:58')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, TIME_TABLE_1_TIMESTAMP_COL, TIME_TABLE_1_TIME_COL)"
        "VALUES ('ROOT000000', 10, '2023-10-11 14:15:16', '14:15:16')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, TIME_TABLE_1_TIMESTAMP_COL, TIME_TABLE_1_TIME_COL)"
        "VALUES ('ROOT000000', 11, '1995-05-07 06:07:08', '06:07:08')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID) VALUES ('ROOT000000', 12)")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"ALLTYPES\" SET TIME_TABLE_1_TIMESTAMP_COL = '1995-05-07 06:07:08' WHERE ID = 6")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"ALLTYPES\" SET TIME_TABLE_1_TIME_COL = '06:07:08' WHERE ID = 5")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DEVPCB\".\"ALLTYPES\" WHERE ID = 4")
    ims_test.ims_db.connection.commit()
    ims_test.ims_db.sync_command()
    ims_test.monitor_page.insert_check('6', '6')
    ims_test.monitor_page.update_check('0', '2')
    ims_test.monitor_page.delete_check('1', '1')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.source_tasklog_path(), ims_test.task_logs_dir,f"reptask_{ims_test.task_name}.log", ims_test.config)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Time.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Time.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Time.csv")