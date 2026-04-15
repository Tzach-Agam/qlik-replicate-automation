from settings import *

def test_variant_binary_control_char(ims_test):
    """VARIANT binary while control field is char"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT3\"")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, COL1)"
    "VALUES('ROOT000003', '00000000000000000001', 'AAAAAA', 'OP1')")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, COL1)"
    "VALUES('ROOT000003', '00000000000000000002', 'BBBBBB', 'OP2')")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, COL1)"
    "VALUES('ROOT000003', '00000000000000000003', 'AAAAAA', 'OP1')")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, COL1)"
    "VALUES('ROOT000003', '00000000000000000004', 'BBBBBB', 'OP2')")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, COL1)"
    "VALUES('ROOT000003', '00000000000000000005', 'AAAAAA', 'OP1')")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY)"
    "VALUES('ROOT000003', '00000000000000000006')")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('1')
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, COL1)"
                                   "VALUES('ROOT000003', '00000000000000000007', 'AAAAAA', 'OP1')")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, COL1)"
                                   "VALUES('ROOT000003', '00000000000000000008', 'BBBBBB', 'OP2')")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, COL1)"
                                   "VALUES('ROOT000003', '00000000000000000009', 'AAAAAA', 'OP1')")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, COL1)"
                                   "VALUES('ROOT000003', '00000000000000000010', 'BBBBBB', 'OP2')")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, COL1)"
                                   "VALUES('ROOT000003', '00000000000000000011', 'AAAAAA', 'OP1')")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY)"
                                   "VALUES('ROOT000003', '00000000000000000012')")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"STRUCT3\" SET VARIANT_SELECTOR = 'BBBBBB', COL2 = 'UP' WHERE SKEY = '00000000000000000001'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"STRUCT3\" SET COL1 = 'UP' WHERE SKEY = '00000000000000000003'")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DVPCB\".\"STRUCT3\" WHERE SKEY = '00000000000000000002'")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.insert_check('6')
    ims_test.monitor_page.update_check('2')
    ims_test.monitor_page.delete_check('1')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions, ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_VAR_BINARY_CON_CHAR.csv")
    # compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_VAR_BINARY_CON_CHAR.good",
    #               ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_VAR_BINARY_CON_CHAR.csv")