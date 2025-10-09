from time import sleep

from configurations.config_manager import ConfigurationManager
from browsers.browsers import get_webdriver
from replicate_pages import *
from utilities.utility_functions import move_file_to_target_dir, log_finder

config = ConfigurationManager("configurations/config.ini")
driver = get_webdriver(config)
replicate_actions=ReplicateCommonActions(driver, config)
replicate_actions.open_replicate_software()
replicate_actions.set_windows_size()
driver.implicitly_wait(3)
replicate_actions.loader_icon_opening_replicate()
tasks_view_page = TasksPage(driver)
replicate_actions.navigate_to_main_page('tasks')
# replicate_actions.delete_task_endpoint('TASK1', 'IMS1', 'Oracle1')
# replicate_actions.delete_task_endpoint('TASK2', 'IMS2', 'Oracle2')
#replicate_actions.delete_task_endpoint('TASK3', 'IMS3', 'Oracle3')
# replicate_actions.delete_task_endpoint('TASK4', 'IMS4', 'Oracle4')
replicate_actions.delete_task_endpoint('TASK5', 'IMS5', 'Oracle5')
# tasks_view_page.open_task("IMS2OracleTrgARR_DATA871896")
# designer_page = DesignerPage(driver)
# designer_page.enter_monitor_page()
# monitor_page = MonitorPage(driver)
# monitor_page.cdc_tab()
# monitor_page.wait_for_message_in_ui("An error occurred during capture changes")


# move_file_to_target_dir(
#     config.replicate_logs_path(),
#     "C:\\Users\juj\PycharmProjects\qlik-replicate-automation\\tests\ims\\array_datatype\\array_datatypes\\task_logs",
#     "reptask_xml_datatype_3_1757584206_22256.log", config, replicate_actions, task_name="xml_datatype_3_1757584206_22256")
# log_finder("C:\\Users\juj\PycharmProjects\qlik-replicate-automation\\tests\ims\\array_datatype\\array_datatypes\\task_logs\\reptask_xml_datatype_3_1757584206_22256.log",
#            "Client session", "C:\\Users\juj\PycharmProjects\qlik-replicate-automation\\tests\ims\\array_datatype\\array_datatypes\good_files\IMS_2_OracleTrg_ARRAY_DATATYPES.csv")
