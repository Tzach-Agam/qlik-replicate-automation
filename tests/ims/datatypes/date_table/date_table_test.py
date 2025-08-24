from settings import *

def test_date_datatype(ims_test):
    """Test for IMS DATE Data Type"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"ALLTYPES\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, DATE_TABLE_1_DATE_COL1, DATE_TABLE_1_DATE_COL2)"
        "VALUES ('ROOT000000', 1, '1999-08-26', '1999-08-26')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, DATE_TABLE_1_DATE_COL1, DATE_TABLE_1_DATE_COL2)"
        "VALUES ('ROOT000000', 2, '2000-01-01', '2000-01-01')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, DATE_TABLE_1_DATE_COL1, DATE_TABLE_1_DATE_COL2)"
        "VALUES ('ROOT000000', 3, '1985-06-15', '1985-06-15')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, DATE_TABLE_1_DATE_COL1, DATE_TABLE_1_DATE_COL2)"
        "VALUES ('ROOT000000', 4, '2024-02-29', '2024-02-29')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, DATE_TABLE_1_DATE_COL1, DATE_TABLE_1_DATE_COL2)"
        "VALUES ('ROOT000000', 5, '1970-01-01', '1970-01-01')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID) VALUES ('ROOT000000', 6)")
    ims_test.ims_db.connection.commit()
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, DATE_TABLE_1_DATE_COL1, DATE_TABLE_1_DATE_COL2)"
        "VALUES ('ROOT000000', 7, '2010-12-25', '2010-12-25')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, DATE_TABLE_1_DATE_COL1, DATE_TABLE_1_DATE_COL2)"
        "VALUES ('ROOT000000', 8, '1969-12-31', '1969-12-31')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, DATE_TABLE_1_DATE_COL1, DATE_TABLE_1_DATE_COL2)"
        "VALUES ('ROOT000000', 9, '2023-11-10', '2023-11-10')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, DATE_TABLE_1_DATE_COL1, DATE_TABLE_1_DATE_COL2)"
        "VALUES ('ROOT000000', 10, '1995-07-05', '1995-07-05')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, DATE_TABLE_1_DATE_COL1, DATE_TABLE_1_DATE_COL2)"
        "VALUES ('ROOT000000', 11, '1988-03-20', '1988-03-20')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID) VALUES ('ROOT000000', 12)")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"ALLTYPES\" SET DATE_TABLE_1_DATE_COL1 = '1988-03-20', DATE_TABLE_1_DATE_COL2 = '1988-03-20' WHERE ID = 5")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"ALLTYPES\" SET DATE_TABLE_1_DATE_COL1 = '1995-07-05', DATE_TABLE_1_DATE_COL2 = '1995-07-05' WHERE ID = 6")
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
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Date.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Date.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Date.csv")