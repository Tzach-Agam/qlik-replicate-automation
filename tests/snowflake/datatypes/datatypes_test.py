from tests.snowflake.snowflake_setup_env import *

def test_number(snow_test):
    create_task(snow_test, "Snowflake_2_Oracle_Number")
    snow_test.snowflake_db.execute(f"CREATE TABLE \"{snow_test.source_schema}\".\"{snow_test.table}\" (ID INT PRIMARY KEY, SMALL_NUM SMALLINT, TINY_NUM TINYINT, BYTE_NUM BYTEINT, INT_NUM INT, BIG_NUM BIGINT, DEC_NUM DECIMAL(10,4), NUMERIC_NUM NUMERIC(15,6), NUMBER_COL NUMBER(20,8)) WITH CHANGE_TRACKING = TRUE;")
    snow_test.snowflake_db.cursor.execute(
    f"""
    INSERT INTO \"{snow_test.source_schema}\".\"{snow_test.table}\" VALUES 
    (1, -32768, -128, -128, -2147483648, -9223372036854775808, -999999.9999, -999999999.999999, -999999999999.99999999),
    (2, 32767, 127, 127, 2147483647, 9223372036854775807, 999999.9999, 999999999.999999, 999999999999.99999999), 
    (3, 0, 0, 0, 0, 0, 0.0001, 0.000001, 0.00000001),
    (4, 30000, 120, 100, 2000000000, 8000000000000000000, 123456.1234, 987654321.987654, 987654321987.12345678),
    (5, 100, 10, 20, 1000, 10000, 1.1, 1.123456, 1.12345678),
    (6, 200, 20, 40, 2000, 20000, 12.34, 12.345678, 12.34567890), 
    (7, 500, 50, 80, 5000, 50000, 123.4567, 123.987654, 123.99999999), 
    (8, 10, 5, 10, 999, 999999, 9.9999, 9.999999, 9.99999999),
    (9, 7, 3, 5, 1234, 1234567890, 12.3400, 12.345678, 1234567890.98765432),
    (10, 32766, 126, 126, 2147483646, 9223372036854775806, 999999.9998, 999999999.999998, 999999999999.99999998), 
    (11, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    """
    )
    snow_test.snowflake_db.connection.commit()
    snow_test.designer_page.run_new_task()
    snow_test.monitor_page.wait_for_fl('1')
    snow_test.monitor_page.cdc_tab()
    snow_test.monitor_page.stop_task()
    snow_test.monitor_page.stop_task_wait()
    snow_test.replicate_actions.navigate_to_main_page('tasks')

def test_something2(snow_test):
    create_task(snow_test, "Snowflake_2_Oracle_Number")
    snow_test.replicate_actions.navigate_to_main_page('tasks')
