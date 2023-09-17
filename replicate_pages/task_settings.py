from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains, Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utilities.utility_functions import safe_click

class TaskSettings:
    """ The TaskSettings class represents the 'Task Settings' dialog box on Qlik Replicate. In it, you configure
        task-specific replication settings.
        The class will provide methods for the task settings page functionality, like setting target replication schemas
        or adding logs"""
    def __init__(self, driver: WebDriver):
        self.driver = driver
        self.actions = ActionChains(driver)

    def metadata_tab(self):
        """Click on the Metadata tab for configuring the target tables metadata"""

        metadata_tab_element = self.driver.find_element(By.XPATH, "//*[text()='Metadata']")
        safe_click(metadata_tab_element)

    def target_schema_click(self):
        """Click on 'Target Metadata' sub-tab under 'Metadata' tab"""

        target_schema_element = self.driver.find_element(By.XPATH, "//*[text()='Target Metadata']")
        safe_click(target_schema_element)

    def target_schema(self, schema: str):
        """ Enter the 'target schema' for the replication task. The 'target schema' is the schema in which the replicated
            tables will be created under.
            :param schema: The name of the 'target schema'"""

        target_schema_element = self.driver.find_element(By.CSS_SELECTOR, "[ng-model='vm.currentFullTask.task_settings.target_settings.default_schema']")
        target_schema_value = target_schema_element.get_attribute("value")
        if target_schema_value:
            target_schema_element.clear()
            target_schema_element.send_keys(schema)
        else:
            target_schema_element.send_keys(schema)

    def control_tables_click(self):
        """Click on 'Control Tables' sub-tab under 'Metadata' tab"""

        control_tables_element = self.driver.find_element(By.XPATH, "//*[text()='Control Tables']")
        safe_click(control_tables_element)

    def control_schema(self, schema: str):
        """ Enter the 'control tables schema' for the replication task. The 'control tables schema' is a schema that is
            created in the target as part of the replication task, and it contains the 'control tables'. Control Tables
            provide information about the replication task like errors and changes.
            :param schema: The name of the 'control tables schema'"""

        control_schema_element = self.driver.find_element(By.CSS_SELECTOR, "[ng-model='vm.currentFullTask.task_settings.target_settings.control_schema']")
        control_schema_value = control_schema_element.get_attribute("value")
        if control_schema_value:
            control_schema_element.clear()
            control_schema_element.send_keys(schema)
        else:
            control_schema_element.send_keys(schema)

    def full_load_tab(self):
        """Click on the 'Full Load' tab for configuration of FL-related settings."""

        fl_tab_element = self.driver.find_element(By.XPATH, "//*[text()='Full Load']" )
        safe_click(fl_tab_element)

    def change_processing(self):
        """Click on the 'Change Processing' tab for configuration of CDC-related settings."""

        change_processing_element = self.driver.find_element(By.XPATH, "//*[text()='Change Processing']")
        safe_click(change_processing_element)

    def store_changes(self):
        """ Turn 'Storge Changes Processing' on to include the 'Store Changes' table in the replication task. The 'Store
            Changes' table will be created in the target database and will contain all the changes done (DML's + DDL's)
            on the replicated tables. """

        store_changes_element = self.driver.find_element(By.XPATH, "//*[text()='Store Changes Settings']")
        safe_click(store_changes_element)
        store_changes_on = self.driver.find_element(By.CSS_SELECTOR, "[class='StoreChanges toggleButton']")
        safe_click(store_changes_on)

    def change_processing_tuning(self):
        """Click on 'Change Processing Tuning' sub-tab under 'Change Processing' tab"""

        change_processing_tuning_element = self.driver.find_element(By.XPATH, "//*[text()='Change Processing Tuning']")
        safe_click(change_processing_tuning_element)

    def transactional_mode_change(self):
        """ Change the Processing mode of CDC events from the default 'Batch optimized apply' to 'Transactional apply'  """

        self.change_processing()
        self.change_processing_tuning()
        processing_mode = self.driver.find_element(By.XPATH, "//*[text()='Batch optimized apply']")
        safe_click(processing_mode)
        transactional = self.driver.find_element(By.XPATH, "//*[text()='Transactional apply']")
        safe_click(transactional)

    def task_logging(self):
        """ Click on 'Logging' tab to change the logging levels of the task """

        wait = WebDriverWait(self.driver, 40)
        element = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Logging']")))
        safe_click(element)

    def change_component_logging(self, *components, logging_level: str):
        """ Change the logging level for specified components.
            This method allows you to change the logging level for one or more components in a Qlik Replicate. You can
            specify one or more components and the desired logging level to be applied.
            :param components: The names of the components for which the logging level should be changed.
            :param logging_level: The desired logging level to be set for the specified components. Supported levels are
            'TRACE' and 'VERBOSE'. """

        try:
            for component in components:
                for i in range(1, 28):
                    logging_element = self.driver.find_element(By.XPATH,
                                                               f"//*[@id='configByLogContainer']/div[{i}]/div[1]/span")
                    logging_element_text = logging_element.text
                    if component.upper() == logging_element_text.upper():
                        if logging_level.upper() == "TRACE":
                            logging_slider = self.driver.find_element(By.XPATH,
                                                                      f"//*[@id='{component.upper()}']/span")
                            actions = ActionChains(self.driver)
                            actions.click_and_hold(logging_slider).move_by_offset(80, 0).release().perform()
                        elif logging_level.upper() == "VERBOSE":
                            logging_slider = self.driver.find_element(By.XPATH,
                                                                      f"//*[@id='{component.upper()}']/span")
                            actions = ActionChains(self.driver)
                            actions.click_and_hold(logging_slider).move_by_offset(132, 0).release().perform()
                        else:
                            raise ValueError(f"Logging level '{logging_level}' not recognized")
        except Exception as e:
            raise Exception("Drag operation was unsuccessful.") from e

    def ok_button(self):
        """Click the 'OK' button on 'task settings' while saving the current settings and closes 'task settings'."""

        ok_button_element = self.driver.find_element(By.XPATH, "//*[text()='OK']")
        safe_click(ok_button_element)

    def cancel_button(self):
        """Click the 'Cancel button on 'task settings' while not saving the current settings and closes 'task settings'."""

        cancel_button_element = self.driver.find_element(By.XPATH, "//*[text()='Cancel']")
        safe_click(cancel_button_element)

    def set_task_settings_general(self, target_schema: str, control_schema: str):
        """ Set the general task settings including target schema and control schema. This method combines previous methods
            in order to set default task settings for the automated replication task."""

        self.target_schema(target_schema)
        self.control_tables_click()
        self.control_schema(control_schema)
        self.ok_button()
