from settings import *

def test_parent_3_children(ims_test):
    """Tests the relationship between a task and it's several children"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC1'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC2'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC3'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC4'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC5'")
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT2\"")
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT3\"")
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT4\"")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ROOT\"  (ROOTID, FILL_0) VALUES ('ROOT000PC1', 'Parent_Child1')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ROOT\"  (ROOTID, FILL_0) VALUES ('ROOT000PC2', 'Parent_Child2')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ROOT\"  (ROOTID, FILL_0) VALUES ('ROOT000PC3', 'Parent_Child3')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ROOT\"  (ROOTID, FILL_0) VALUES ('ROOT000PC4', 'Parent_Child4')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"ROOT\"  (ROOTID, FILL_0) VALUES ('ROOT000PC5', 'Parent_Child5')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY1', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY2', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY3', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC2', 'KEY1', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC2', 'KEY2', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC2', 'KEY3', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY1', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY2', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY3', 3333, 'CCCCC')")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY1', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY2', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY3', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC3', 'KEY1', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC3', 'KEY2', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC3', 'KEY3', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY1', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY2', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY3', 3333, 'CCCCC')")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY1', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY2', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY3', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC4', 'KEY1', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC4', 'KEY2', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC4', 'KEY3', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY1', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY2', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY3', 3333, 'CCCCC')")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('4')
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY4', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY5', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY6', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC2', 'KEY4', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC2', 'KEY5', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC2', 'KEY6', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY4', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY5', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY6', 3333, 'CCCCC')")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY4', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY5', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY6', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC3', 'KEY4', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC3', 'KEY5', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC3', 'KEY6', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY4', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY5', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT3\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY6', 3333, 'CCCCC')")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY4', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY5', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC1', 'KEY6', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC4', 'KEY4', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC4', 'KEY5', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC4', 'KEY6', 3333, 'CCCCC')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY4', 1111, 'AAAAA')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY5', 2222, 'BBBBB')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT4\"  (ROOT_ROOTID, SKEY, COL1_NUM, COL2_CHAR) VALUES ('ROOT000PC5', 'KEY6', 3333, 'CCCCC')")

    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC1'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC5'")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('0', '9', '9', '9')
    ims_test.monitor_page.delete_check('2', '12', '12', '12')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions, ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}Parent_3_Children.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}Parent_3_Children.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}Parent_3_Children.csv")