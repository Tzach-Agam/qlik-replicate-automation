from settings import *

def test_variant_string(ims_test):
    """Test for IMS basic VARIANT scenario"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT2\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000001', 'ABC', 11111, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000002', 'ABC', 22222, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000003', 'ABC', 33333, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000004', 'ABC', 44444, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000005', 'ABC', 55555, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000006', 'ABC', 66666, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000007', 'ABC', 77777, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000008', 'ABC', 88888, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000009', 'XYZ', 'BETA', 99999)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000010', 'XYZ', 'BETA', 10101)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY)"
        "VALUES ('ROOT000002', '00000000000000000111')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000222', 'XYZ', 11111, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000333', 'ABC', 'BETA', 11111)")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('1')
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000011', 'ABC', 11111, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000012', 'ABC', 22222, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000013', 'XYZ', 'BETA', 13131)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)" 
        "VALUES ('ROOT000002', '00000000000000000014', 'XYZ', 'BETA', 14141)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)" 
        "VALUES ('ROOT000002', '00000000000000000015', 'XYZ', 'BETA', 15151)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)" 
        "VALUES ('ROOT000002', '00000000000000000016', 'XYZ', 'BETA', 16161)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)" 
        "VALUES ('ROOT000002', '00000000000000000017', 'XYZ', 'BETA', 17171)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)" 
        "VALUES ('ROOT000002', '00000000000000000018', 'XYZ', 'BETA', 18181)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)" 
        "VALUES ('ROOT000002', '00000000000000000019', 'XYZ', 'BETA', 19191)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)" 
        "VALUES ('ROOT000002', '00000000000000000020', 'XYZ', 'BETA', 20202)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY)"
        "VALUES ('ROOT000002', '00000000000000000444')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_A1, FIELD_A2)"
        "VALUES ('ROOT000002', '00000000000000000555', 'XYZ', 11111, 'ALPHA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\" (ROOT_ROOTID, SKEY, VARIANT_SELECTOR, FIELD_B1, FIELD_B2)"
        "VALUES ('ROOT000002', '00000000000000000666', 'ABC', 'BETA', 11111)")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"STRUCT2\" SET VARIANT_SELECTOR = 'XYZ' WHERE SKEY = '00000000000000000001'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"STRUCT2\" SET VARIANT_SELECTOR = 'XYZ', FIELD_B1 = 'UPDA' WHERE SKEY = '00000000000000000002'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DEVPCB\".\"STRUCT2\" SET VARIANT_SELECTOR = 'XYZ', FIELD_B1 = 'UPDA', FIELD_B2 = 12345 WHERE SKEY = '00000000000000000003'")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DEVPCB\".\"STRUCT2\" WHERE SKEY = '00000000000000000004'")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DEVPCB\".\"STRUCT2\" WHERE SKEY = '00000000000000000005'")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.insert_check('13')
    ims_test.monitor_page.update_check('3')
    ims_test.monitor_page.delete_check('2')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions, ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_VARIANT_STRING.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_VARIANT_STRING.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_VARIANT_STRING.csv")