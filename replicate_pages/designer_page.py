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
        self.actions = ActionChains(driver)

    def choose_source_target(self, source_endpoint_data: dict, target_endpoint_data: dict):
        """ Choose source and target endpoints for a task by their names on the 'Endpoint Connections' list in Designer Mode.
            The names will be taken from a dictionary that contain the endpoint's definition.
            :param source_endpoint_data: Information about the source endpoint.
            :param target_endpoint_data: Information about the target endpoint."""

        source_name = source_endpoint_data["name"]
        target_name = target_endpoint_data["name"]
        self.driver.find_element(By.XPATH, f"//*[text()='{source_name}']/following-sibling::span[1]").click()
        self.driver.find_element(By.XPATH, f"//*[text()='{target_name}']/following-sibling::span[1]").click()

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

    def enter_monitor_page(self):
        """Enter to the task's 'Monitor Mode', where you view the replication task activities in real time."""

        monitor = self.driver.find_element(By.XPATH, "//span[text()='Monitor']")
        safe_click(monitor)

    def run_task_dropdown(self):
        """Click the 'Run' task dropdown menu."""

        run_dropdown = self.driver.find_element(By.CSS_SELECTOR, "button[data-toggle='dropdown']>[class='caret']")
        safe_click(run_dropdown)

    def run_new_task(self):
        """ Start a new task by clicking on the 'Start Processing' option in the run options dropdown. """

        self.run_task_dropdown()
        start_processing = self.driver.find_element(By.XPATH,
                                                    "//li[@title='Start Processing']/a[text()='Start Processing']")
        safe_click(start_processing)

    def start_task_wait(self):
        """Wait for the task to start and provide status messages."""

        wait = WebDriverWait(self.driver, 20)
        try:
            wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Starting task']")))
            print("Starting task")
            wait.until(EC.invisibility_of_element_located((By.XPATH, "//*[text()='Starting task']")))
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

        wait = WebDriverWait(self.driver, 20)
        try:
            wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Stopping task']")))
            print("Stopping task.")
            wait.until(EC.invisibility_of_element_located((By.XPATH, "//*[text()='Stopping task']")))
            print("Task stopped")
        except:
            print("Element did not become visible within the timeout or became stale.")
