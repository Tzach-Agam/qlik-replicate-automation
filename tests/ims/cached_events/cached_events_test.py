from settings import *

def test_cached_events(ims_test):
    """Test for cached events"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT1\"")
    for i in range(1, 2001):
        ims_test.ims_db.cursor.execute(
            f"INSERT INTO STRUCT1 (ROOT_ROOTID, SKEY, NUM_DATES, VARIANT_SELECTOR, NUM_1, STR_1, DATE_STRUCT_1_DATE_YY, DATE_STRUCT_1_DATE_MM, DATE_STRUCT_1_DATE_DD )"
            f"VALUES ('ROOT000001', 'KEY{i}', 1, 'A', 100001, 'AAAAAAAAAA', 1999, 08, 26)")
    ims_test.ims_db.connection.commit()
    print("Finished with first batch")
    for i in range(2001, 4001):
        ims_test.ims_db.cursor.execute(
        f"INSERT INTO STRUCT1 (ROOT_ROOTID, SKEY, NUM_DATES, VARIANT_SELECTOR, SHORT_2, STR_2, DATE_STRUCT_1_DATE_YY, DATE_STRUCT_1_DATE_MM, DATE_STRUCT_1_DATE_DD, DATE_STRUCT_2_DATE_YY, DATE_STRUCT_2_DATE_MM, DATE_STRUCT_2_DATE_DD)"
        f"VALUES ('ROOT000001', 'KEY{i}', 2, 'B', 111, 'TZACH', 1999, 08, 26, 2000, 09, 21)")
    ims_test.ims_db.connection.commit()
    print("Finished with second batch")
    ims_test.designer_page.run_new_task()
    for i in range(4001, 4011):
        ims_test.ims_db.cursor.execute(
            f"INSERT INTO STRUCT1 (ROOT_ROOTID, SKEY, NUM_DATES, VARIANT_SELECTOR, NUM_1, STR_1, DATE_STRUCT_1_DATE_YY, DATE_STRUCT_1_DATE_MM, DATE_STRUCT_1_DATE_DD, DATE_STRUCT_2_DATE_YY, DATE_STRUCT_2_DATE_MM, DATE_STRUCT_2_DATE_DD, DATE_STRUCT_3_DATE_YY, DATE_STRUCT_3_DATE_MM, DATE_STRUCT_3_DATE_DD)" 
            f"VALUES ('ROOT000001', 'KEY{i}', 3, 'A', 100002, 'SHLOMO', 1999, 08, 26, 2000, 09, 21, 2010, 10, 10)")
    for i in range(1, 11):
        ims_test.ims_db.cursor.execute(
            f"UPDATE STRUCT1 SET STR_1 = 'UPDATEEE' WHERE SKEY = 'KEY{i}'")
    for i in range(11, 21):
        ims_test.ims_db.cursor.execute(
            f"DELETE FROM STRUCT1 WHERE SKEY = 'KEY{i}'")
    ims_test.ims_db.connection.commit()
    ims_test.ims_db.sync_command()
    ims_test.monitor_page.wait_for_fl('2', 60)
    ims_test.monitor_page.cdc_tab()