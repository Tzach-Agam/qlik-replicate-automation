from settings import *

def test_bit_datatype(ims_test):
    """Test for IMS BIT Data Type"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"ALLTYPES\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 1, 1, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 2, 1, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 3, 0, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 4, 0, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 5, 1, 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 6, 1, 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 7, 0, 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)" 
        f"VALUES ('ROOT000000', 8, 0, 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID)" 
        f"VALUES ('ROOT000000', 9)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID)" 
        f"VALUES ('ROOT000000', 10)")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)"
        f"VALUES ('ROOT000000', 11, 1, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)"
        f"VALUES ('ROOT000000', 12, 1, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)"
        f"VALUES ('ROOT000000', 13, 0, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)"
        f"VALUES ('ROOT000000', 14, 0, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)"
        f"VALUES ('ROOT000000', 15, 1, 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)"
        f"VALUES ('ROOT000000', 16, 1, 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)"
        f"VALUES ('ROOT000000', 17, 0, 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BIT_TABLE_1_BIT_COL1, BIT_TABLE_1_BIT_COL2)"
        f"VALUES ('ROOT000000', 18, 0, 0)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID)"
        f"VALUES ('ROOT000000', 19)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID)"
        f"VALUES ('ROOT000000', 20)")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET BIT_TABLE_1_BIT_COL1 = 1 WHERE ID = 4")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET BIT_TABLE_1_BIT_COL2 = 1 WHERE ID = 6")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET BIT_TABLE_1_BIT_COL1 = 1 WHERE ID = 8")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET BIT_TABLE_1_BIT_COL1 = 1 WHERE ID = 10")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET BIT_TABLE_1_BIT_COL1 = 0 WHERE ID = 16")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DVPCB\".\"ALLTYPES\" WHERE ID = 2")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DVPCB\".\"ALLTYPES\" WHERE ID = 20")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('10', '8')
    ims_test.monitor_page.update_check('0', '2')
    ims_test.monitor_page.delete_check('2', '2')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions, ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Bit.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Bit.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Bit.csv")