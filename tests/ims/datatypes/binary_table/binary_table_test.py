from settings import *

def test_binary_datatype(ims_test):
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"ALLTYPES\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ('ROOT000000', 1,"
        f"    X'000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D',"
        f"    X'1F1E1D1C1B1A191817161514131211100F0E0D0C0B0A0908070605040302',"
        f"    X'01', X'FF')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ('ROOT000000', 2,"
        f"    X'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',"
        f"    X'000000000000000000000000000000000000000000000000000000000000',"
        f"    X'00', X'00')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ('ROOT000000', 3,"
        f"    X'AAAAAAAA55555555AAAAAAAA55555555AAAAAAAA55555555AAAAAAAA5555',"
        f"    X'55555555AAAAAAAA55555555AAAAAAAA55555555AAAAAAAA55555555AAAA',"
        f"    X'7F', X'80')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ( 'ROOT000000', 4,"
        f"    X'000000000000000000000000000000000000000000000000000000000000',"
        f"    X'0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E',"
        f"    X'FE', X'11')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ( 'ROOT000000', 5,"
        f"    X'1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890AB',"
        f"    X'FEDCBA0987654321FEDCBA0987654321FEDCBA0987654321FEDCBA098765',"
        f"    X'AA', X'55')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID) VALUES ('ROOT000000', 6)")
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ('ROOT000000', 7,"
        f"    X'000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D',"
        f"    X'1F1E1D1C1B1A191817161514131211100F0E0D0C0B0A0908070605040302',"
        f"    X'01', X'FF')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ('ROOT000000', 8,"
        f"    X'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',"
        f"    X'000000000000000000000000000000000000000000000000000000000000',"
        f"    X'00', X'00')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ('ROOT000000', 9,"
        f"    X'AAAAAAAA55555555AAAAAAAA55555555AAAAAAAA55555555AAAAAAAA5555',"
        f"    X'55555555AAAAAAAA55555555AAAAAAAA55555555AAAAAAAA55555555AAAA',"
        f"    X'7F', X'80')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ('ROOT000000', 10,"
        f"    X'000000000000000000000000000000000000000000000000000000000000',"
        f"    X'0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E',"
        f"    X'FE', X'11')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ( 'ROOT000000', 11,"
        f"    X'1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890AB',"
        f"    X'FEDCBA0987654321FEDCBA0987654321FEDCBA0987654321FEDCBA098765',"
        f"    X'AA', X'55')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID) VALUES ('ROOT000000', 12)")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"ALLTYPES\" SET BINARY_TABLE_1_BINARY_COL1 = X'CB34567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890CB' WHERE ID = 6")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"ALLTYPES\" SET BINARY_TABLE_1_BINARY_COL2 = X'CB34567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890CB' WHERE ID = 5")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DEVPCB\".\"ALLTYPES\" WHERE ID = 8")
    ims_test.ims_db.connection.commit()
    ims_test.ims_db.sync_command()
    ims_test.monitor_page.insert_check('6', '6')
    ims_test.monitor_page.update_check('0', '2')
    ims_test.monitor_page.delete_check('1', '1')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.source_tasklog_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Binary.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Binary.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Binary.csv")