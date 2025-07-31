from tests.snowflake.snowflake_setup_env import *

def test_something(snow_test):
    create_task(snow_test, "Snowflake_2_Oracle_Number")
    snow_test.replicate_actions.navigate_to_main_page('tasks')

def test_something2(snow_test):
    create_task(snow_test, "Snowflake_2_Oracle_Number")
    snow_test.replicate_actions.navigate_to_main_page('tasks')
