from tests.mssql.sql_env_setup import *

def test_char_datatype(mssql_test):
    create_task(mssql_test, "SQL2Oracle_Chars_Datatype")
    mssql_test.mssql_db.execute_query(f'CREATE TABLE "{mssql_test.source_schema}".test_table (A int primary key, char1 char(36), varchar1 varchar(36), nchar1 nchar(36), nvarchar1 nvarchar(36));')
    mssql_test.mssql_db.cursor.execute(
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (1, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (2, '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (3, 'a', 'a', 'a', 'a');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (4, '', '', '', '');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (5, ' ', ' ', ' ', ' ');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (6, ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (7, 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (8, null,null,null,null);")
    mssql_test.mssql_db.connection.commit()
    mssql_test.designer_page.run_new_task()
    mssql_test.monitor_page.wait_for_fl('2')
    mssql_test.monitor_page.cdc_tab()
    mssql_test.mssql_db.cursor.execute(
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (9, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (10, '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (11, 'a', 'a', 'a', 'a');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (12, '', '', '', '');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (13, ' ', ' ', ' ', ' ');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (14, ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (15, 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ');"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (16, null,null,null,null);")
    mssql_test.mssql_db.connection.commit()
    mssql_test.mssql_db.cursor.execute(
    f"DELETE FROM \"{mssql_test.source_schema}\".test_table WHERE A =5;"
    f"UPDATE \"{mssql_test.source_schema}\".test_table SET char1='', varchar1='', nchar1='', nvarchar1='' WHERE A=1;"
	f"UPDATE \"{mssql_test.source_schema}\".test_table SET char1='amich',  varchar1='amich', nchar1='amich', nvarchar1='amich' WHERE A=4;")
    mssql_test.mssql_db.connection.commit()
    mssql_test.mssql_db.sync_command(mssql_test.source_schema, mssql_test.sync_table)
    mssql_test.monitor_page.insert_check('1', '8')
    mssql_test.monitor_page.update_check('0', '2')
    mssql_test.monitor_page.delete_check('0', '1')
    mssql_test.monitor_page.wait_for_cdc()
    mssql_test.monitor_page.stop_task()
    mssql_test.monitor_page.stop_task_wait()
    mssql_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(mssql_test.config.source_tasklog_path(), mssql_test.task_logs_dir,f"reptask_{mssql_test.task_name}.log", mssql_test.config)
    mssql_test.oracle_db.export_schema_data_to_csv(mssql_test.target_schema, mssql_test.good_files_dir + "\\SQL_2_Oracle_char_datatype.csv")
    compare_files(mssql_test.good_files_dir + "\\SQL_2_Oracle_char_datatype.good", mssql_test.good_files_dir + "\\SQL_2_Oracle_char_datatype.csv")

def test_several_tables(mssql_test):
    create_task(mssql_test, "SQL2Oracle_Several_Tables")
    for i in range(1,4):
        mssql_test.mssql_db.execute_query(f'CREATE TABLE "{mssql_test.source_schema}".test_table{i} (A int primary key, char1 char(36), varchar1 varchar(36), nchar1 nchar(36), nvarchar1 nvarchar(36));')
    for i in range(1,4):
        mssql_test.mssql_db.cursor.execute(
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (1, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (2, '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (3, 'a', 'a', 'a', 'a');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (4, '', '', '', '');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (5, ' ', ' ', ' ', ' ');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (6, ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (7, 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (8, null,null,null,null);")
    mssql_test.mssql_db.connection.commit()
    mssql_test.designer_page.run_new_task()
    mssql_test.monitor_page.wait_for_fl('3')
    mssql_test.monitor_page.cdc_tab()
    for i in range(1, 4):
        mssql_test.mssql_db.cursor.execute(
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (9, 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (10, '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890', '!@#$%^&*()_-+=[]<>?.,1234567890');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (11, 'a', 'a', 'a', 'a');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (12, '', '', '', '');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (13, ' ', ' ', ' ', ' ');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (14, ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa', ' aaaaaaaaaaaaaaaaaaaa');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (15, 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ', 'zzzzzzzzzzzzzzzzzzzz ');"
            f"INSERT INTO \"{mssql_test.source_schema}\".test_table{i} VALUES (16, null,null,null,null);")
    mssql_test.mssql_db.connection.commit()
    for i in range(1, 4):
        mssql_test.mssql_db.cursor.execute(
        f"DELETE FROM \"{mssql_test.source_schema}\".test_table{i} WHERE A =5;"
        f"UPDATE \"{mssql_test.source_schema}\".test_table{i} SET char1='', varchar1='', nchar1='', nvarchar1='' WHERE A=1;"
	    f"UPDATE \"{mssql_test.source_schema}\".test_table{i} SET char1='amich',  varchar1='amich', nchar1='amich', nvarchar1='amich' WHERE A=4;"
        )
    mssql_test.mssql_db.connection.commit()
    mssql_test.monitor_page.insert_check('8', '8', '8')
    mssql_test.monitor_page.update_check('2', '2', '2')
    mssql_test.monitor_page.delete_check('1', '1', '1')
    mssql_test.monitor_page.stop_task()
    mssql_test.monitor_page.stop_task_wait()
    mssql_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(mssql_test.config.source_tasklog_path(), mssql_test.task_logs_dir,f"reptask_{mssql_test.task_name}.log", mssql_test.config)
    mssql_test.oracle_db.export_schema_data_to_csv(mssql_test.target_schema, mssql_test.good_files_dir + "\\SQL_2_Oracle_char_datatype.csv")
    compare_files(mssql_test.good_files_dir + "\\SQL_2_Oracle_char_datatype.good", mssql_test.good_files_dir + "\\SQL_2_Oracle_char_datatype.csv")


def test_number_datatype(mssql_test):
    create_task(mssql_test, "SQL2Oracle_Number_Datatype")
    mssql_test.mssql_db.execute_query(
        f'CREATE TABLE "{mssql_test.source_schema}".test_table (a int primary key, decimal_1 DECIMAL(18,0), decimal_2 DECIMAL(9,5), decimal_3 DECIMAL(38,0), decimal_4 DECIMAL(38,38), int_1 INT, money_1 MONEY, smallint_1 SMALLINT, numeric_1 NUMERIC (5), numeric_2 NUMERIC (15,5), numeric_3 NUMERIC (25,5), numeric_4 NUMERIC (30,5), smallmoney_1 SMALLMONEY, tinyint_1 TINYINT, bigint_1 BIGINT);'
    )
    mssql_test.mssql_db.cursor.execute(
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (1, 999999999999999999, 9999.99999, 99999999999999999999999999999999999999, 0.99999999999999999999999999999999999999, -2147483648, -922337203685477.5808, -32768, -99999, 1234567890.54321, 123456789012345.54321, 12345678901234567890.54321, - 214748.3648, 255, -9225808);"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (2, 1, 1.99999, 1, 0.00000000000000000000000000000000000001, 2147483647, 922337203685477.5807, 32767, 99999, 9999999999.99999, 999999999999999.99999, 99999999999999999999.99999, 214748.3647, 88, 9223372036854775807);"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (3, 123456789 , 1000.88545, 123456789, 0.123456789, 256, 461168601842738.79035, 12345, 1, 1, 1, -99999999999999999999.99999, 0.9985, 1, 1);"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (4, -999999999999999999, -9999.99999, 123456789, 0.999999999999999999, 256, 461168601842738.79035, 0, 0, 0, 0, 0, 0, 0, 0);"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (5, null, null, null, null, null, null, null, null, null, null, null, null, null, null);"
        f"INSERT INTO \"{mssql_test.source_schema}\".test_table VALUES (6, -999999999999999999, -9999.99999, -99999999999999999999999999999999999999, -0.99999999999999999999999999999999999999, 2147483647, 922337203685477.5807, 32767, 99999, -9999999999.99999, -99999999999999999999.99999, -9999999999999999999999999.99999, 214748.3647, 0, -9223372036854775808);"
    )
    mssql_test.mssql_db.connection.commit()
    mssql_test.designer_page.run_new_task()
    mssql_test.monitor_page.wait_for_fl('1')
    mssql_test.monitor_page.cdc_tab()
    mssql_test.monitor_page.stop_task()
    mssql_test.monitor_page.stop_task_wait()
    mssql_test.replicate_actions.navigate_to_main_page('tasks')