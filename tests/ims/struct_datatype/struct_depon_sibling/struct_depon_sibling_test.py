from time import sleep

from settings import *

def test_struct_depon_array_sibling(ims_test):
    """Test when the depends on of an array is in a struct that is the sibling of the array struct"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"STRUCT4\"")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY1', 1, 12, 'MAY', 1999)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY2', 2, 12, 'MAY', 1999, 24, 'APRIL', 2000)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR,"
                                   "ARRAY_DATE_3_ARRAY_DAY, ARRAY_DATE_3_ARRAY_MONTH, ARRAY_DATE_3_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY3', 3, 12, 'MAY', 1999, 24, 'APRIL', 2000, 15, 'AUGUST', 2025)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY4', 0, 5, 'JANUARY', 1980)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY5', 1, 18, 'FEBRUARY', 1995, 7, 'MARCH', 2001)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR,"
                                   "ARRAY_DATE_3_ARRAY_DAY, ARRAY_DATE_3_ARRAY_MONTH, ARRAY_DATE_3_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY6', 2, 22, 'APRIL', 1977, 9, 'MAY', 1988, 30, 'JUNE', 1999)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY7', 2, 14, 'JULY', 2003)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY8', 3, 3, 'AUGUST', 2010, 25, 'SEPTEMBER', 2012)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR,"
                                   "ARRAY_DATE_3_ARRAY_DAY, ARRAY_DATE_3_ARRAY_MONTH, ARRAY_DATE_3_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY9', 4, 11, 'OCTOBER', 1965, 6, 'NOVEMBER', 1970, 27, 'DECEMBER', 1985)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY)" 
                                   "VALUES ('ROOT000004', 'KEY10')")
    ims_test.ims_db.connection.commit()
    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('2')
    ims_test.monitor_page.cdc_tab()
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY11', 1, 12, 'MAY', 1999)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY12', 2, 12, 'MAY', 1999, 24, 'APRIL', 2000)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR,"
                                   "ARRAY_DATE_3_ARRAY_DAY, ARRAY_DATE_3_ARRAY_MONTH, ARRAY_DATE_3_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY13', 3, 12, 'MAY', 1999, 24, 'APRIL', 2000, 15, 'AUGUST', 2025)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY14', 0, 5, 'JANUARY', 1980)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY15', 1, 18, 'FEBRUARY', 1995, 7, 'MARCH', 2001)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR,"
                                   "ARRAY_DATE_3_ARRAY_DAY, ARRAY_DATE_3_ARRAY_MONTH, ARRAY_DATE_3_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY16', 2, 22, 'APRIL', 1977, 9, 'MAY', 1988, 30, 'JUNE', 1999)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY17', 2, 14, 'JULY', 2003)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY18', 3, 3, 'AUGUST', 2010, 25, 'SEPTEMBER', 2012)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY, DEP_ON_FIELD,"
                                   "ARRAY_DATE_1_ARRAY_DAY, ARRAY_DATE_1_ARRAY_MONTH, ARRAY_DATE_1_ARRAY_YEAR,"
                                   "ARRAY_DATE_2_ARRAY_DAY, ARRAY_DATE_2_ARRAY_MONTH, ARRAY_DATE_2_ARRAY_YEAR,"
                                   "ARRAY_DATE_3_ARRAY_DAY, ARRAY_DATE_3_ARRAY_MONTH, ARRAY_DATE_3_ARRAY_YEAR)"
                                   "VALUES ('ROOT000004', 'KEY19', 4, 11, 'OCTOBER', 1965, 6, 'NOVEMBER', 1970, 27, 'DECEMBER', 1985)")
    ims_test.ims_db.cursor.execute(f"INSERT INTO \"DVPCB\".\"STRUCT4\" (ROOT_ROOTID, SKEY)" 
                                   "VALUES ('ROOT000004', 'KEY20')")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DVPCB\".\"STRUCT4\" SET DEP_ON_FIELD = 2, ARRAY_DATE_2_ARRAY_DAY = 26, ARRAY_DATE_2_ARRAY_MONTH = 'August Update', ARRAY_DATE_2_ARRAY_YEAR = 1999 WHERE SKEY = 'KEY4'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DVPCB\".\"STRUCT4\" SET DEP_ON_FIELD = 2, ARRAY_DATE_1_ARRAY_MONTH = 'August Update', ARRAY_DATE_2_ARRAY_YEAR = 1999 WHERE SKEY = 'KEY8'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DVPCB\".\"STRUCT4\" SET DEP_ON_FIELD = 2, ARRAY_DATE_1_ARRAY_MONTH = 'May Update', ARRAY_DATE_2_ARRAY_DAY = 26, ARRAY_DATE_2_ARRAY_MONTH = 'August Update', ARRAY_DATE_2_ARRAY_YEAR = 1999 WHERE SKEY = 'KEY1'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"STRUCT4\" WHERE  SKEY = 'KEY3'")
    ims_test.ims_db.connection.commit()
    ims_test.ims_db.sync_command()
    ims_test.monitor_page.insert_check('20', '10')
    ims_test.monitor_page.update_check('3', '3')
    ims_test.monitor_page.delete_check('4', '1')
    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()
    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions, ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_STRUCT_DEPON_ARRAY_SIBLING.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_STRUCT_DEPON_ARRAY_SIBLING.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_STRUCT_DEPON_ARRAY_SIBLING.csv")
