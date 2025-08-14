from settings import *

def test_something(ims_test):
    create_task(ims_test, "IMS_2_Oracle_Char")
    ims_test.replicate_actions.navigate_to_main_page('tasks')