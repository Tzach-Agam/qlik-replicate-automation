from settings import *

def test_variant_with_array(ims_test):
    """Test for variant with ARRAY scenario"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT1\"")
    for i in range(1, 20):
        ims_test.ims_db.cursor.execute(
            f"INSERT INTO STRUCT1 (ROOT_ROOTID, SKEY, NUM_DATES, VARIANT_SELECTOR, NUM_1, STR_1, DATE_STRUCT_1_DATE_YY, DATE_STRUCT_1_DATE_MM, DATE_STRUCT_1_DATE_DD )"
            f"VALUES ('ROOT000001', 'KEY{i}', 1, 'A', 100001, 'AAAAAAAAAA', 1999, 08, 26)")
    ims_test.ims_db.connection.commit()
    for i in range(21, 40):
        ims_test.ims_db.cursor.execute(
        f"INSERT INTO STRUCT1 (ROOT_ROOTID, SKEY, NUM_DATES, VARIANT_SELECTOR, SHORT_2, STR_2, DATE_STRUCT_1_DATE_YY, DATE_STRUCT_1_DATE_MM, DATE_STRUCT_1_DATE_DD, DATE_STRUCT_2_DATE_YY, DATE_STRUCT_2_DATE_MM, DATE_STRUCT_2_DATE_DD)"
        f"VALUES ('ROOT000001', 'KEY{i}', 2, 'B', 111, 'TZACH', 1999, 08, 26, 2000, 09, 21)")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2', 60)
    ims_test.monitor_page.cdc_tab()

    for i in range(41, 50):
        ims_test.ims_db.cursor.execute(
            f"INSERT INTO STRUCT1 (ROOT_ROOTID, SKEY, NUM_DATES, VARIANT_SELECTOR, NUM_1, STR_1, DATE_STRUCT_1_DATE_YY, DATE_STRUCT_1_DATE_MM, DATE_STRUCT_1_DATE_DD, DATE_STRUCT_2_DATE_YY, DATE_STRUCT_2_DATE_MM, DATE_STRUCT_2_DATE_DD, DATE_STRUCT_3_DATE_YY, DATE_STRUCT_3_DATE_MM, DATE_STRUCT_3_DATE_DD)" 
            f"VALUES ('ROOT000001', 'KEY{i}', 3, 'A', 100002, 'SHLOMO', 1999, 08, 26, 2000, 09, 21, 2010, 10, 10)")
    for i in range(1, 6):
        ims_test.ims_db.cursor.execute(
            f"UPDATE STRUCT1 SET STR_1 = 'UPDATE' WHERE SKEY = 'KEY{i}'")
    for i in range(21, 26):
        ims_test.ims_db.cursor.execute(
            f"UPDATE STRUCT1 SET STR_2 = 'UPDAT' WHERE SKEY = 'KEY{i}'")
    for i in range(35, 41):
        ims_test.ims_db.cursor.execute(
            f"DELETE FROM STRUCT1 WHERE SKEY = 'KEY{i}'")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('9', '27')
    ims_test.monitor_page.update_check('10', '0')
    ims_test.monitor_page.delete_check('5', '10')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    collect_logs(ims_test)
    finalize_test(ims_test, f"IMS_2_{ims_test.target_db.config['endpoint']}_VARIANT_WITH_ARRAY")