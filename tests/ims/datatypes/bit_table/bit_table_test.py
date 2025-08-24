from settings import *

def test_bit_datatype(ims_test):
    """Test for IMS BIT Data Type"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"ALLTYPES\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 1, 1, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 2, 0, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 3, 0, 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 4, 1, 0)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID)" 
                                   f"VALUES ('ROOT000000', 5)")
    ims_test.ims_db.connection.commit()
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 6, 1, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 7, 0, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 8, 0, 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 9, 1, 0)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID)" 
                                   f"VALUES ('ROOT000000', 10)")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"ALLTYPES\" SET BIT_TABLE_1_BIT_COL1 = 1 WHERE ID = 5")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"ALLTYPES\" SET BIT_TABLE_1_BIT_COL1 = 0 WHERE ID = 4")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DEVPCB\".\"ALLTYPES\" WHERE ID = 3")
    ims_test.ims_db.connection.commit()
    ims_test.ims_db.sync_command()
    ims_test.monitor_page.insert_check('5', '5')
    ims_test.monitor_page.update_check('0', '2')
    ims_test.monitor_page.delete_check('1', '1')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.source_tasklog_path(), ims_test.task_logs_dir,f"reptask_{ims_test.task_name}.log", ims_test.config)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Bit.csv")
    # compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Char.good",
    #               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Char.csv")