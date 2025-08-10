from tests.mssql.sql_env_setup import *

def test_char_datatype(mssql_test):
    create_task(mssql_test, "SQL2Oracle_Chars_Datatype")
    mssql_test.mssql_db.execute_query(
        f'CREATE TABLE "{mssql_test.source_schema}".test_table (A int primary key, char1 char(36), varchar1 varchar(36), nchar1 nchar(36), nvarchar1 nvarchar(36));')
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
    mssql_test.monitor_page.wait_for_fl('1')
    mssql_test.monitor_page.cdc_tab()
    mssql_test.monitor_page.stop_task()
    mssql_test.monitor_page.stop_task_wait()
    mssql_test.replicate_actions.navigate_to_main_page('tasks')

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