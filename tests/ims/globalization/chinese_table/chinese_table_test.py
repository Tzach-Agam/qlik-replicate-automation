from settings import *

def test_chinese_table(ims_test):
    """Replication of chinese characters from IMS."""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT2\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, CHINESE_TABLE_1_COL1, CHINESE_TABLE_1_COL2)"
        "VALUES ('ROOT000002', 'KEY1', X'E6B189E8AFADE6BCA2', X'E6B189E8AFADE6BCA2')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, CHINESE_TABLE_1_COL1, CHINESE_TABLE_1_COL2)"
        "VALUES ('ROOT000002', 'KEY2', X'E6B189E8AFADE6BCA2', X'E6B189E8AFADE6BCA2')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, CHINESE_TABLE_1_COL1, CHINESE_TABLE_1_COL2)"
        "VALUES ('ROOT000002', 'KEY3', X'E6B189E8AFADE6BCA2', X'E6B189E8AFADE6BCA2')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, CHINESE_TABLE_1_COL1, CHINESE_TABLE_1_COL2)"
        "VALUES ('ROOT000002', 'KEY4', X'E6B189E8AFADE6BCA2', X'E6B189E8AFADE6BCA2')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, CHINESE_TABLE_1_COL1, CHINESE_TABLE_1_COL2)"
        "VALUES ('ROOT000002', 'KEY5', X'E6B189E8AFADE6BCA2', X'E6B189E8AFADE6BCA2')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, CHINESE_TABLE_1_COL1, CHINESE_TABLE_1_COL2)"
        "VALUES ('ROOT000002', 'KEY6', X'E6B189E8AFADE6BCA2', X'E6B189E8AFADE6BCA2')")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, CHINESE_TABLE_1_COL1, CHINESE_TABLE_1_COL2)"
        "VALUES ('ROOT000002', 'KEY7', X'E6B189E8AFADE6BCA2', X'E6B189E8AFADE6BCA2')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, CHINESE_TABLE_1_COL1, CHINESE_TABLE_1_COL2)"
        "VALUES ('ROOT000002', 'KEY8', X'E6B189E8AFADE6BCA2', X'E6B189E8AFADE6BCA2')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, CHINESE_TABLE_1_COL1, CHINESE_TABLE_1_COL2)"
        "VALUES ('ROOT000002', 'KEY9', X'E6B189E8AFADE6BCA2', X'E6B189E8AFADE6BCA2')")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DVPCB\".\"STRUCT2\"  SET CHINESE_TABLE_1_COL1 = X'E6BCA2E6BCA2E6BCA2' WHERE SKEY = 'KEY1'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DVPCB\".\"STRUCT2\"  SET CHINESE_TABLE_1_COL1 = X'E6BCA2E6BCA2E6BCA2' WHERE SKEY = 'KEY2'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"STRUCT2\"  WHERE SKEY = 'KEY4'")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('3', '3')
    ims_test.monitor_page.update_check('0', '2')
    ims_test.monitor_page.delete_check('1', '1')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    collect_logs(ims_test)
    finalize_test(ims_test, f"IMS_2_{ims_test.target_db.config['endpoint']}_CHINESE_TABLE")