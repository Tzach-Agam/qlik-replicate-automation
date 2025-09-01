from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from utilities.utility_functions import safe_click
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class DesignerPage:
    """ The DesignerPage represents the 'Designer mode' on Qlik Replicate. In it, you define endpoints, select tables to
        be replicated and modify table and task settings. This is the default mode when you open a task.
        The DesignerPage class will provide various functionalities on Designer Mode, including running tasks, selecting
        endpoints and, entering to the pages 'Task Settings', 'Manage Endpoint', 'Table Selection' and 'Monitor Mode'."""

    def __init__(self, driver: WebDriver):
        """ Initialize the DesignerPage object
            :param driver: WebDriver instance for Selenium automation. """
        self.driver = driver
        self.wait = WebDriverWait(self.driver, 20)
        self.actions = ActionChains(self.driver)

    def choose_source_target(self, source_endpoint: str, target_endpoint: str):
        """ Choose source and target endpoints for a task by their names on the 'Endpoint Connections' list in Designer Mode.
            The names are create dynamically during the task start
            :param source_endpoint: Information about the source endpoint.
            :param target_endpoint: Information about the target endpoint."""
        self.driver.find_element(By.XPATH, f"//*[text()='{source_endpoint}']/following-sibling::span[1]").click()
        self.driver.find_element(By.XPATH, f"//*[text()='{target_endpoint}']/following-sibling::span[1]").click()

    def enter_task_settings(self):
        """Enter to 'Task Settings' dialog box, where the task-specific replication settings will be configured."""
        task_settings = self.driver.find_element(By.XPATH, "//span[text()='Task Settings...']")
        safe_click(task_settings)

    def enter_manage_endpoints(self):
        """ Enter to 'Manage Endpoint Connections' window, the endpoint management page, where an endpoint is created,
         edited and configured. """
        manage_endpoints = self.driver.find_element(By.XPATH, "//span[text()='Manage Endpoint Connections...']")
        safe_click(manage_endpoints)

    def enter_table_selection(self):
        """ Enter to 'Table Selection' window - where schemas and tables will be selected for the replication task. """
        table_selection = self.driver.find_element(By.XPATH, "//span[text()='Table Selection...']")
        safe_click(table_selection)

    def save_task(self):
        """Saves the configured task."""
        save_button = self.driver.find_element(By.XPATH, "//span[text()='Save']")
        safe_click(save_button)

    def enter_chosen_source(self):
        """Double-click on the chosen source endpoint."""
        source_element = self.driver.find_element(By.CSS_SELECTOR, ".dbItemDetails.ng-scope:nth-child(1)")
        self.actions.double_click(source_element).perform()

    def enter_chosen_target(self):
        """Double-click on the chosen target endpoint."""
        target_element = self.driver.find_element(By.CSS_SELECTOR, ".dbItemDetails.ng-scope:nth-child(2)")
        self.actions.double_click(target_element).perform()

    def enter_chosen_table_settings(self, table_name):
        """Enters to certain tables table settings"""
        full_table_list = self.driver.find_element(By.XPATH, "//*[@id='tabSwapper']/ul/li[2]/div[1]/a/span")
        full_table_list.click()
        table_element  = self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(), '{table_name}')]")))
        table_element.click()
        table_settings_element = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Table Settings...']")))
        table_settings_element.click()
        self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "[class='loader-inner ball-clip-rotate-multiple']")))
        self.wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "[class='loader-inner ball-clip-rotate-multiple']")))
        self.wait.until(EC.visibility_of_element_located((By.XPATH, f"//*[contains(text(), '{table_name}') and contains(text(), 'Table Settings')]")))

    def enter_monitor_page(self):
        """Enter to the task's 'Monitor Mode'."""
        monitor = self.driver.find_element(By.XPATH, "//span[text()='Monitor']")
        safe_click(monitor)

    def run_task_dropdown(self):
        """Click the 'Run' task dropdown menu."""
        run_dropdown = self.driver.find_element(By.CSS_SELECTOR, "button[data-toggle='dropdown']>[class='caret']")
        safe_click(run_dropdown)

    def run_new_task(self, timeout=40):
        """ Start a new task by clicking on the 'Start Processing' option in the run options dropdown. """
        self.run_task_dropdown()
        start_processing = self.driver.find_element(By.XPATH,
                                                    "//li[@title='Start Processing']/a[text()='Start Processing']")
        safe_click(start_processing)
        try:
            dynamic_wait = WebDriverWait(self.driver, timeout)
            dynamic_wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Starting task']")))
            print("Starting task")
            dynamic_wait.until(EC.invisibility_of_element_located((By.XPATH, "//*[text()='Starting task']")))
            print("Task started")
        except:
            print("Element did not become visible within the timeout or became stale.")

    def stop_task(self):
        """Stop the task entirely."""
        stop_task_element = self.driver.find_element(By.XPATH, "//span[text()='Stop']")
        safe_click(stop_task_element)
        yes_button = self.driver.find_element(By.XPATH, "//button[text()='Yes']")
        safe_click(yes_button)

    def stop_task_wait(self):
        """Wait for the task to stop and provide status messages."""
        try:
            self.wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Stopping task']")))
            print("Stopping task.")
            self.wait.until(EC.invisibility_of_element_located((By.XPATH, "//*[text()='Stopping task']")))
            print("Task stopped")
        except:
            print("Element did not become visible within the timeout or became stale.")
