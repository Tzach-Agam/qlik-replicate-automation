from settings import *

def test_float_datatype(ims_test):
    """Test for IMS FLOAT Data Type"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"ALLTYPES\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 1, 123.456, 12.34)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 2, -98765.4321, -1234.56)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 3, 0.00012345, 0.001234)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 4, -0.00098765, -0.009876)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 5, 1.23456789E10, 1.2345E5)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID)"
        "VALUES('ROOT000000', 6)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 7, 123.456, 12.34)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 8, -98765.4321, -1234.56)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 9, 0.00012345, 0.001234)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 10, -0.00098765, -0.009876)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 11, 1.23456789E10, 1.2345E5)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID)"
        "VALUES('ROOT000000', 12)")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 13, -9.87654321E8, -98765.4)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 14, 3.1415926535, 3.1416)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 15, -2.718281828, -2.7183)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 16, 6.02214076E23, 1.602E-19)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, FLOAT_TABLE_1_DOUBLE_COL, FLOAT_TABLE_1_FLOAT_COL)"
        "VALUES ('ROOT000000', 17, -1.6180339887, -0.6180)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID) VALUES ('ROOT000000', 18)")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET FLOAT_TABLE_1_DOUBLE_COL = 1.082699 WHERE ID = 12")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET FLOAT_TABLE_1_FLOAT_COL = 1.3456 WHERE ID = 11")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET FLOAT_TABLE_1_DOUBLE_COL = 3.1415926535 WHERE ID = 10")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DVPCB\".\"ALLTYPES\" WHERE ID = 9")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('6', '6')
    ims_test.monitor_page.update_check('0', '2')
    ims_test.monitor_page.delete_check('1', '1')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    collect_logs(ims_test)
    finalize_test(ims_test, f"IMS_2_{ims_test.target_db.config['endpoint']}_Float")