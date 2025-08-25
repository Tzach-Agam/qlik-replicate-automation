from settings import *

def test_array_with_dep(ims_test):
    """Test for IMS ARRAY WITH dependency"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT2\"")
    # Elements like depends on
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_1COL_1_COL1)" 
        "VALUES ('ROOT000002', 'KEY1', 1, 99, 08, 26, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1)" 
        "VALUES ('ROOT000002', 'KEY2', 2, 99, 08, 26, 01, 12, 27, 'AAAAA', 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_3_DATE_YY, DATE_ARRAY_3_DATE_MM, DATE_ARRAY_3_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1, DATE_ARRAY_1COL_3_COL1)" 
        "VALUES ('ROOT000002', 'KEY3', 3, 99, 08, 26, 01, 12, 27, 22, 10, 12, 'AAAAA', 'BBBBB', 'CCCCC')")
    # More elements than depends on
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_1COL_1_COL1)" 
        "VALUES ('ROOT000002', 'KEY4', 0, 99, 08, 26, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1)" 
        "VALUES ('ROOT000002', 'KEY5', 1, 99, 08, 26, 01, 12, 27, 'AAAAA', 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_3_DATE_YY, DATE_ARRAY_3_DATE_MM, DATE_ARRAY_3_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1, DATE_ARRAY_1COL_3_COL1)" 
        "VALUES ('ROOT000002', 'KEY6', 2, 99, 08, 26, 01, 12, 27, 22, 10, 12, 'AAAAA', 'BBBBB', 'CCCCC')")
    #Fewer elements than depends on
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_1COL_2_COL1)" 
        "VALUES ('ROOT000002', 'KEY7', 2, 99, 08, 26, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1)" 
        "VALUES ('ROOT000002', 'KEY8', 3, 99, 08, 26, 01, 12, 27, 'AAAAA', 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_3_DATE_YY, DATE_ARRAY_3_DATE_MM, DATE_ARRAY_3_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1, DATE_ARRAY_1COL_3_COL1)" 
        "VALUES ('ROOT000002', 'KEY9', 4, 99, 08, 26, 01, 12, 27, 22, 10, 12, 'AAAAA', 'BBBBB', 'CCCCC')")
    # More values combinations
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY)"
                                   "VALUES ('ROOT000002', 'KEY10')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM)" 
                                   "VALUES ('ROOT000002', 'KEY11', 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_3_DATE_YY, DATE_ARRAY_3_DATE_MM, DATE_ARRAY_3_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1, DATE_ARRAY_1COL_3_COL1)" 
        "VALUES ('ROOT000002', 'KEY12', 99, 08, 26, 01, 12, 27, 22, 10, 12, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.connection.commit()
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()
    # Elements like depends on
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_1COL_1_COL1)"
        "VALUES ('ROOT000002', 'KEY13', 1, 99, 08, 26, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1)"
        "VALUES ('ROOT000002', 'KEY14', 2, 99, 08, 26, 01, 12, 27, 'AAAAA', 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_3_DATE_YY, DATE_ARRAY_3_DATE_MM, DATE_ARRAY_3_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1, DATE_ARRAY_1COL_3_COL1)"
        "VALUES ('ROOT000002', 'KEY15', 3, 99, 08, 26, 01, 12, 27, 22, 10, 12, 'AAAAA', 'BBBBB', 'CCCCC')")
    # More elements than depends on
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_1COL_1_COL1)"
        "VALUES ('ROOT000002', 'KEY16', 0, 99, 08, 26, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1)"
        "VALUES ('ROOT000002', 'KEY17', 1, 99, 08, 26, 01, 12, 27, 'AAAAA', 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_3_DATE_YY, DATE_ARRAY_3_DATE_MM, DATE_ARRAY_3_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1, DATE_ARRAY_1COL_3_COL1)"
        "VALUES ('ROOT000002', 'KEY18', 2, 99, 08, 26, 01, 12, 27, 22, 10, 12, 'AAAAA', 'BBBBB', 'CCCCC')")
    # Fewer elements than depends on
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_1COL_2_COL1)"
        "VALUES ('ROOT000002', 'KEY19', 2, 99, 08, 26, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1)"
        "VALUES ('ROOT000002', 'KEY20', 3, 99, 08, 26, 01, 12, 27, 'AAAAA', 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_3_DATE_YY, DATE_ARRAY_3_DATE_MM, DATE_ARRAY_3_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1, DATE_ARRAY_1COL_3_COL1)"
        "VALUES ('ROOT000002', 'KEY21', 4, 99, 08, 26, 01, 12, 27, 22, 10, 12, 'AAAAA', 'BBBBB', 'CCCCC')")
    # More values combinations
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY)"
                                   "VALUES ('ROOT000002', 'KEY22')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATES_NUM)"
                                   "VALUES ('ROOT000002', 'KEY23', 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, DATE_ARRAY_1_DATE_YY, DATE_ARRAY_1_DATE_MM, DATE_ARRAY_1_DATE_DD, DATE_ARRAY_2_DATE_YY, DATE_ARRAY_2_DATE_MM, DATE_ARRAY_2_DATE_DD, DATE_ARRAY_3_DATE_YY, DATE_ARRAY_3_DATE_MM, DATE_ARRAY_3_DATE_DD, DATE_ARRAY_1COL_1_COL1, DATE_ARRAY_1COL_2_COL1, DATE_ARRAY_1COL_3_COL1)"
        "VALUES ('ROOT000002', 'KEY24', 99, 08, 26, 01, 12, 27, 22, 10, 12, 'AAAAA', 'BBBBB', 'CCCCC')")
    #Adds another value to KEY1
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\" SET DATES_NUM = 2, DATE_ARRAY_1_DATE_YY = 11, DATE_ARRAY_1_DATE_MM = 11, DATE_ARRAY_1_DATE_DD = 11, DATE_ARRAY_2_DATE_YY = 22, DATE_ARRAY_2_DATE_MM = 22, DATE_ARRAY_2_DATE_DD = 22, DATE_ARRAY_1COL_1_COL1 = 'UPDAT', DATE_ARRAY_1COL_2_COL1 = 'UPDAT'  WHERE SKEY = 'KEY1'")
    #Removes one value from KEY2
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\" SET DATES_NUM = 1, DATE_ARRAY_1_DATE_YY = 22, DATE_ARRAY_1_DATE_MM = 22, DATE_ARRAY_1_DATE_DD = 22, DATE_ARRAY_1COL_1_COL1 = 'UPDAT' WHERE SKEY = 'KEY2'")
    #UPDATES EXISTING value in KEY3
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\" SET DATES_NUM = 3, DATE_ARRAY_1_DATE_YY = 33, DATE_ARRAY_1_DATE_MM = 33, DATE_ARRAY_1_DATE_DD = 33, DATE_ARRAY_1COL_1_COL1 = 'UPDAT', DATE_ARRAY_2_DATE_YY = 33, DATE_ARRAY_2_DATE_MM = 33, DATE_ARRAY_2_DATE_DD = 33, DATE_ARRAY_1COL_2_COL1 = 'UPDAT', DATE_ARRAY_3_DATE_YY = 33, DATE_ARRAY_3_DATE_MM = 33, DATE_ARRAY_3_DATE_DD = 33, DATE_ARRAY_1COL_3_COL1 = 'UPDAT' WHERE SKEY = 'KEY3'")
    #Chanes value NUM in KEY4 to 1 but adds values to second element
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\" SET DATES_NUM = 1, DATE_ARRAY_2_DATE_YY = 44, DATE_ARRAY_2_DATE_MM = 44, DATE_ARRAY_2_DATE_DD = 44, DATE_ARRAY_1COL_2_COL1 = 'UPDAT' WHERE SKEY = 'KEY4'")
    #Updates KEY10 by adding 1 to NUM but to the second element
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\" SET DATES_NUM = 1, DATE_ARRAY_2_DATE_YY = 10, DATE_ARRAY_2_DATE_MM = 10, DATE_ARRAY_2_DATE_DD = 10, DATE_ARRAY_1COL_2_COL1 = 'UPDAT' WHERE SKEY = 'KEY10'")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DEVPCB\".\"STRUCT2\" WHERE SKEY = 'KEY6'")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DEVPCB\".\"STRUCT2\" WHERE SKEY = 'KEY12'")
    ims_test.ims_db.connection.commit()
    ims_test.ims_db.sync_command()
    ims_test.monitor_page.insert_check('17', '12')
    ims_test.monitor_page.update_check('5', '5')
    ims_test.monitor_page.delete_check('3', '2')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.source_tasklog_path(), ims_test.task_logs_dir,f"reptask_{ims_test.task_name}.log", ims_test.config)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_WITH_DEP.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_WITH_DEP.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_WITH_DEP.csv")