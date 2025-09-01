from settings import *

def test_array_no_dep(ims_test):
    """Test for IMS ARRAY without dependency"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT2\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, AR_NOD_1C_1E_1_COL1, AR_NOD_1C_3E_1_COL2, AR_NOD_1C_3E_2_COL2, AR_NOD_1C_3E_3_COL2, AR_NOD_3C_1E_1_COL3,AR_NOD_3C_1E_1_COL4,AR_NOD_3C_1E_1_COL5, AR_NOD_3C_3E_1_COL6,AR_NOD_3C_3E_1_COL7,AR_NOD_3C_3E_1_COL8,AR_NOD_3C_3E_2_COL6,AR_NOD_3C_3E_2_COL7,AR_NOD_3C_3E_2_COL8,AR_NOD_3C_3E_3_COL6,AR_NOD_3C_3E_3_COL7,AR_NOD_3C_3E_3_COL8) "
        f"VALUES ('ROOT000002', 'KEY1', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA', 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, AR_NOD_1C_1E_1_COL1, AR_NOD_1C_3E_1_COL2, AR_NOD_1C_3E_2_COL2, AR_NOD_1C_3E_3_COL2, AR_NOD_3C_1E_1_COL3,AR_NOD_3C_1E_1_COL4,AR_NOD_3C_1E_1_COL5, AR_NOD_3C_3E_1_COL6,AR_NOD_3C_3E_1_COL7,AR_NOD_3C_3E_1_COL8,AR_NOD_3C_3E_2_COL6,AR_NOD_3C_3E_2_COL7,AR_NOD_3C_3E_2_COL8,AR_NOD_3C_3E_3_COL6,AR_NOD_3C_3E_3_COL7,AR_NOD_3C_3E_3_COL8) "
        f"VALUES ('ROOT000002', 'KEY2', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB', 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, AR_NOD_1C_1E_1_COL1, AR_NOD_1C_3E_1_COL2, AR_NOD_1C_3E_2_COL2, AR_NOD_1C_3E_3_COL2, AR_NOD_3C_1E_1_COL3,AR_NOD_3C_1E_1_COL4,AR_NOD_3C_1E_1_COL5, AR_NOD_3C_3E_1_COL6,AR_NOD_3C_3E_1_COL7,AR_NOD_3C_3E_1_COL8,AR_NOD_3C_3E_2_COL6,AR_NOD_3C_3E_2_COL7,AR_NOD_3C_3E_2_COL8,AR_NOD_3C_3E_3_COL6,AR_NOD_3C_3E_3_COL7,AR_NOD_3C_3E_3_COL8) "
        f"VALUES ('ROOT000002', 'KEY3', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, AR_NOD_1C_1E_1_COL1, AR_NOD_1C_3E_1_COL2, AR_NOD_1C_3E_2_COL2, AR_NOD_1C_3E_3_COL2, AR_NOD_3C_1E_1_COL3,AR_NOD_3C_1E_1_COL4,AR_NOD_3C_1E_1_COL5, AR_NOD_3C_3E_1_COL6,AR_NOD_3C_3E_1_COL7,AR_NOD_3C_3E_1_COL8,AR_NOD_3C_3E_2_COL6,AR_NOD_3C_3E_2_COL7,AR_NOD_3C_3E_2_COL8,AR_NOD_3C_3E_3_COL6,AR_NOD_3C_3E_3_COL7,AR_NOD_3C_3E_3_COL8) "
        f"VALUES ('ROOT000002', 'KEY4', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD', 'DDDDD')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, AR_NOD_1C_1E_1_COL1, AR_NOD_1C_3E_1_COL2, AR_NOD_1C_3E_2_COL2, AR_NOD_1C_3E_3_COL2, AR_NOD_3C_1E_1_COL3,AR_NOD_3C_1E_1_COL4,AR_NOD_3C_1E_1_COL5, AR_NOD_3C_3E_1_COL6,AR_NOD_3C_3E_1_COL7,AR_NOD_3C_3E_1_COL8,AR_NOD_3C_3E_2_COL6,AR_NOD_3C_3E_2_COL7,AR_NOD_3C_3E_2_COL8,AR_NOD_3C_3E_3_COL6,AR_NOD_3C_3E_3_COL7,AR_NOD_3C_3E_3_COL8) "
        f"VALUES ('ROOT000002', 'KEY5', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE', 'EEEEE')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY) VALUES ('ROOT000002', 'KEY6')")
    ims_test.ims_db.connection.commit()
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('3')
    ims_test.monitor_page.cdc_tab()
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, AR_NOD_1C_1E_1_COL1, AR_NOD_1C_3E_1_COL2, AR_NOD_1C_3E_2_COL2, AR_NOD_1C_3E_3_COL2, AR_NOD_3C_1E_1_COL3,AR_NOD_3C_1E_1_COL4,AR_NOD_3C_1E_1_COL5, AR_NOD_3C_3E_1_COL6,AR_NOD_3C_3E_1_COL7,AR_NOD_3C_3E_1_COL8,AR_NOD_3C_3E_2_COL6,AR_NOD_3C_3E_2_COL7,AR_NOD_3C_3E_2_COL8,AR_NOD_3C_3E_3_COL6,AR_NOD_3C_3E_3_COL7,AR_NOD_3C_3E_3_COL8) "
        f"VALUES ('ROOT000002', 'KEY7', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF', 'FFFFF')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, AR_NOD_1C_1E_1_COL1, AR_NOD_1C_3E_1_COL2, AR_NOD_1C_3E_2_COL2, AR_NOD_1C_3E_3_COL2, AR_NOD_3C_1E_1_COL3,AR_NOD_3C_1E_1_COL4,AR_NOD_3C_1E_1_COL5, AR_NOD_3C_3E_1_COL6,AR_NOD_3C_3E_1_COL7,AR_NOD_3C_3E_1_COL8,AR_NOD_3C_3E_2_COL6,AR_NOD_3C_3E_2_COL7,AR_NOD_3C_3E_2_COL8,AR_NOD_3C_3E_3_COL6,AR_NOD_3C_3E_3_COL7,AR_NOD_3C_3E_3_COL8) "
        f"VALUES ('ROOT000002', 'KEY8', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG', 'GGGGG')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, AR_NOD_1C_1E_1_COL1, AR_NOD_1C_3E_1_COL2, AR_NOD_1C_3E_2_COL2, AR_NOD_1C_3E_3_COL2, AR_NOD_3C_1E_1_COL3,AR_NOD_3C_1E_1_COL4,AR_NOD_3C_1E_1_COL5, AR_NOD_3C_3E_1_COL6,AR_NOD_3C_3E_1_COL7,AR_NOD_3C_3E_1_COL8,AR_NOD_3C_3E_2_COL6,AR_NOD_3C_3E_2_COL7,AR_NOD_3C_3E_2_COL8,AR_NOD_3C_3E_3_COL6,AR_NOD_3C_3E_3_COL7,AR_NOD_3C_3E_3_COL8) "
        f"VALUES ('ROOT000002', 'KEY9', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH', 'HHHHH')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, AR_NOD_1C_1E_1_COL1, AR_NOD_1C_3E_1_COL2, AR_NOD_1C_3E_2_COL2, AR_NOD_1C_3E_3_COL2, AR_NOD_3C_1E_1_COL3,AR_NOD_3C_1E_1_COL4,AR_NOD_3C_1E_1_COL5, AR_NOD_3C_3E_1_COL6,AR_NOD_3C_3E_1_COL7,AR_NOD_3C_3E_1_COL8,AR_NOD_3C_3E_2_COL6,AR_NOD_3C_3E_2_COL7,AR_NOD_3C_3E_2_COL8,AR_NOD_3C_3E_3_COL6,AR_NOD_3C_3E_3_COL7,AR_NOD_3C_3E_3_COL8) "
        f"VALUES ('ROOT000002', 'KEY10', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII', 'IIIII')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, AR_NOD_1C_1E_1_COL1, AR_NOD_1C_3E_1_COL2, AR_NOD_1C_3E_2_COL2, AR_NOD_1C_3E_3_COL2, AR_NOD_3C_1E_1_COL3,AR_NOD_3C_1E_1_COL4,AR_NOD_3C_1E_1_COL5, AR_NOD_3C_3E_1_COL6,AR_NOD_3C_3E_1_COL7,AR_NOD_3C_3E_1_COL8,AR_NOD_3C_3E_2_COL6,AR_NOD_3C_3E_2_COL7,AR_NOD_3C_3E_2_COL8,AR_NOD_3C_3E_3_COL6,AR_NOD_3C_3E_3_COL7,AR_NOD_3C_3E_3_COL8) "
        f"VALUES ('ROOT000002', 'KEY11', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ', 'JJJJJ')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY) VALUES ('ROOT000002', 'KEY12')")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"STRUCT2\" SET AR_NOD_1C_1E_1_COL1 = 'KKKKK', AR_NOD_1C_3E_1_COL2 = 'KKKKK' , AR_NOD_1C_3E_2_COL2 = 'KKKKK', AR_NOD_1C_3E_3_COL2 = 'KKKKK', AR_NOD_3C_1E_1_COL3 = 'KKKKK', AR_NOD_3C_1E_1_COL4 = 'KKKKK', AR_NOD_3C_1E_1_COL5 = 'KKKKK', AR_NOD_3C_3E_1_COL6  = 'KKKKK', AR_NOD_3C_3E_1_COL7  = 'KKKKK', AR_NOD_3C_3E_1_COL8  = 'KKKKK', AR_NOD_3C_3E_2_COL6 = 'KKKKK', AR_NOD_3C_3E_2_COL7 = 'KKKKK', AR_NOD_3C_3E_2_COL8 = 'KKKKK', AR_NOD_3C_3E_3_COL6 = 'KKKKK', AR_NOD_3C_3E_3_COL7 = 'KKKKK', AR_NOD_3C_3E_3_COL8 = 'KKKKK' WHERE SKEY = 'KEY3'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"STRUCT2\" SET AR_NOD_1C_1E_1_COL1 = 'LLLLL', AR_NOD_1C_3E_1_COL2 = 'LLLLL' , AR_NOD_1C_3E_2_COL2 = 'LLLLL', AR_NOD_1C_3E_3_COL2 = 'LLLLL', AR_NOD_3C_1E_1_COL3 = 'LLLLL', AR_NOD_3C_1E_1_COL4 = 'LLLLL', AR_NOD_3C_1E_1_COL5 = 'LLLLL', AR_NOD_3C_3E_1_COL6  = 'LLLLL', AR_NOD_3C_3E_1_COL7  = 'LLLLL', AR_NOD_3C_3E_1_COL8  = 'LLLLL', AR_NOD_3C_3E_2_COL6 = 'LLLLL', AR_NOD_3C_3E_2_COL7 = 'LLLLL', AR_NOD_3C_3E_2_COL8 = 'LLLLL', AR_NOD_3C_3E_3_COL6 = 'LLLLL', AR_NOD_3C_3E_3_COL7 = 'LLLLL', AR_NOD_3C_3E_3_COL8 = 'LLLLL' WHERE SKEY = 'KEY4'")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DEVPCB\".\"STRUCT2\" WHERE SKEY = 'KEY5'")
    ims_test.ims_db.connection.commit()
    ims_test.ims_db.sync_command()
    ims_test.monitor_page.insert_check('6', '18', '6')
    ims_test.monitor_page.update_check('2', '6', '2')
    ims_test.monitor_page.delete_check('1', '3', '1')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.source_tasklog_path(), ims_test.task_logs_dir,f"reptask_{ims_test.task_name}.log", ims_test.config)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_NO_DEP.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_NO_DEP.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_NO_DEP.csv")