from settings import *

def test_struct_in_struct(ims_test):
    """Test for IMS STRUCT inside a STRUCT"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT4\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY1', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY2', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY3', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY4', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY5', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY6', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('1')
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY7', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY8', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY9', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY10', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY11', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, S1_NUM_COL, S2_NUM_COL, S3_NUM_COL,  S1_CHAR_COL, S2_CHAR_COL, S3_CHAR_COL)"
        "VALUES ('ROOT000004', 'KEY12', 22, 33, 44, 'AAAAA', 'BBBBB', 'CCCCC')")

    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DVPCB\".\"STRUCT4\" SET S1_NUM_COL = 99, S1_CHAR_COL = 'UPDAT' WHERE SKEY = 'KEY1'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DVPCB\".\"STRUCT4\" SET S2_NUM_COL = 99, S2_CHAR_COL = 'UPDAT' WHERE SKEY = 'KEY2'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"STRUCT4\" WHERE SKEY = 'KEY3'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"STRUCT4\" WHERE SKEY = 'KEY4'")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.insert_check('6')
    ims_test.monitor_page.update_check('2')
    ims_test.monitor_page.delete_check('2')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    collect_logs(ims_test)
    finalize_test(ims_test, f"IMS_2_{ims_test.target_db.config['endpoint']}_STRUCT_IN_STRUCT")