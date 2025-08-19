from settings import *

def test_binary_datatype(ims_test):
    create_task(ims_test, "IMS_2_Oracle_Binary_Data")
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"ALLTYPES\"")
    print("Deleted existing data from ALLTYPES table in IMS DB")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ("
        f"    'ROOT000000',"
        f"    1,"
        f"    X'000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D',"
        f"    X'1F1E1D1C1B1A191817161514131211100F0E0D0C0B0A0908070605040302',"
        f"    X'01',"
        f"    X'FF'"
        f")")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ("
        f"    'ROOT000000',"
        f"    2,"
        f"    X'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',"
        f"    X'000000000000000000000000000000000000000000000000000000000000',"
        f"    X'00',"
        f"    X'00'"
        f")")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ("
        f"    'ROOT000000',"
        f"    3,"
        f"    X'AAAAAAAA55555555AAAAAAAA55555555AAAAAAAA55555555AAAAAAAA5555',"
        f"    X'55555555AAAAAAAA55555555AAAAAAAA55555555AAAAAAAA55555555AAAA',"
        f"    X'7F',"
        f"    X'80'"
        f")")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ("
        f"    'ROOT000000',"
        f"    4,"
        f"    X'000000000000000000000000000000000000000000000000000000000000',"
        f"    X'0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E',"
        f"    X'FE',"
        f"    X'11'"
        f")")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ALLTYPES\""
        f"(ROOT_ROOTID, ID, BINARY_TABLE_1_BINARY_COL1, BINARY_TABLE_1_BINARY_COL2, BINARY_TABLE_1_BYTE_COL1, BINARY_TABLE_1_BYTE_COL2)"
        f"VALUES ("
        f"    'ROOT000000',"
        f"    5,"
        f"    X'1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890AB',"
        f"    X'FEDCBA0987654321FEDCBA0987654321FEDCBA0987654321FEDCBA098765',"
        f"    X'AA',"
        f"    X'55'"
        f")")
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.source_tasklog_path(), ims_test.task_logs_dir,f"reptask_{ims_test.task_name}.log", ims_test.config)
    ims_test.oracle_db.export_schema_data_to_csv(ims_test.target_schema, ims_test.good_files_dir + "\\IMS_2_Oracle_Binary_Data.csv")
    #compare_files(ims_test.good_files_dir + "\\IMS_2_Oracle_Binary_Data.good", ims_test.good_files_dir + "\\IMS_2_Oracle_Binary_Data.csv")