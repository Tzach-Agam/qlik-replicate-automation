from settings import *

def test_char_datatype(ims_test):
    """Test for IMS Char Data Type"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"ALLTYPES\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 1, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 2, '!@#$%^&*()_-+=[]<>?.,123456', '!@#$%^&*()_-+=[]<>?.,123456')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 3, 'a', 'a')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 4, '', 'aaaaa')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 5, 'aaaaa', ' ')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 6, 'aaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaa')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 7, 'zzzzzzzzzzzzzzzzzzzz', 'zzzzzzzzzzzzzzzzzzzz')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID) VALUES ('ROOT000000', 8)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 9, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 10, '!@#$%^&*()_-+=[]<>?.,123456', '!@#$%^&*()_-+=[]<>?.,123456')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 11, 'a', 'a')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 12, '', '')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 13, ' ', ' ')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 14, 'aaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaa')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 15, 'zzzzzzzzzzzzzzzzzzzz', 'zzzzzzzzzzzzzzzzzzzz')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID) VALUES ('ROOT000000', 16)")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 17, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 18, '!@#$%^&*()_-+=[]<>?.,123456', '!@#$%^&*()_-+=[]<>?.,123456')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 19, 'a', 'a')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 20, '', 'aaaaa')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 21, 'aaaaa', ' ')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 22, 'aaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaa')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 23, 'zzzzzzzzzzzzzzzzzzzz', 'zzzzzzzzzzzzzzzzzzzz')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID) VALUES ('ROOT000000', 24)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 25, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 26, '!@#$%^&*()_-+=[]<>?.,123456', '!@#$%^&*()_-+=[]<>?.,123456')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 27, 'a', 'a')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 28, '', '')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 29, ' ', ' ')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 30, 'aaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaa')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID, CHAR_TABLE_1_CHAR_COL1, CHAR_TABLE_1_CHAR_COL2) VALUES "
        f"('ROOT000000', 31, 'zzzzzzzzzzzzzzzzzzzz', 'zzzzzzzzzzzzzzzzzzzz')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ALLTYPES\" (ROOT_ROOTID, ID) VALUES ('ROOT000000', 32)")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET CHAR_TABLE_1_CHAR_COL1 = 'updated_value', CHAR_TABLE_1_CHAR_COL2 = 'updated_value' WHERE ID = 3")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET CHAR_TABLE_1_CHAR_COL1 = 'updated_value2', CHAR_TABLE_1_CHAR_COL2 = 'updated_value2' WHERE ID = 5")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET CHAR_TABLE_1_CHAR_COL1 = 'updated_value', CHAR_TABLE_1_CHAR_COL2 = 'updated_value' WHERE ID = 12")
    ims_test.ims_db.cursor.execute(
        f"UPDATE  \"DVPCB\".\"ALLTYPES\" SET CHAR_TABLE_1_CHAR_COL1 = 'updated_value2', CHAR_TABLE_1_CHAR_COL2 = 'updated_value2' WHERE ID = 13")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DVPCB\".\"ALLTYPES\" WHERE ID = 14")
    ims_test.ims_db.cursor.execute(
        f"DELETE FROM \"DVPCB\".\"ALLTYPES\" WHERE ID = 15")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('14', '16')
    ims_test.monitor_page.update_check('2', '0')
    ims_test.monitor_page.delete_check('2', '2')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions, ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema, ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Char.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Char.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_Char.csv")