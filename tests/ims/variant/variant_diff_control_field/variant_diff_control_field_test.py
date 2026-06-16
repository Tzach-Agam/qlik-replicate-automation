from settings import *

def test_variant_diff_control_field(ims_test):
    """VARIANT control field is different from the Case Name"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT2\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000001', 'AL', 11111, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000002', 'AL', 22222, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000003', 'AL', 33333, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000004', 'AL', 44444, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000005', 'AL', 55555, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000006', 'AL', 66666, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000007', 'AL', 77777, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000008', 'AL', 88888, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000009', 'BE', 'BETA', 99999)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000010', 'BE', 'BETA', 10101)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY)"
        "VALUES ('ROOT000002', '00000000000000000111')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000222', 'BE', 11111, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000333', 'AL', 'BETA', 11111)")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('1')
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000011', 'AL', 11111, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000012', 'AL', 22222, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000013', 'BE', 'BETA', 13131)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000014', 'BE', 'BETA', 14141)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000015', 'BE', 'BETA', 15151)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000016', 'BE', 'BETA', 16161)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000017', 'BE', 'BETA', 17171)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY)"
        "VALUES ('ROOT000002', '00000000000000000444')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000555', 'BE', 11111, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000666', 'AL', 'BETA', 11111)")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"STRUCT2\" SET VARIANT_SELECTOR = 'BE' WHERE SKEY = '00000000000000000001'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"STRUCT2\" SET VARIANT_SELECTOR = 'BE', FIELD_B1 = 'UPDA' WHERE SKEY = '00000000000000000002'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"STRUCT2\" SET VARIANT_SELECTOR = 'BE', FIELD_B1 = 'UPDA', FIELD_B2 = 12345 WHERE SKEY = '00000000000000000003'")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DVPCB\".\"STRUCT2\" WHERE SKEY = '00000000000000000004'")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DVPCB\".\"STRUCT2\" WHERE SKEY = '00000000000000000005'")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('10')
    ims_test.monitor_page.update_check('3')
    ims_test.monitor_page.delete_check('2')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    collect_logs(ims_test)
    finalize_test(ims_test, f"IMS_2_{ims_test.target_db.config['endpoint']}_VARIANT_DIFF_CON_FIELD")