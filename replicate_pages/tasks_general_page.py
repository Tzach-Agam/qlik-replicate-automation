from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from utilities.utility_functions import safe_click


class TasksPage:
    """ The TasksPage class Represents the general tasks page in Qlik replicate software, which is default page on
        Replicate.

        Objective: Managing tasks on Qlik Replicate.

        Functionalities: This class provides methods for interacting with various elements on the tasks page, such as
        creating, selecting, opening, and deleting tasks."""

    def __init__(self, driver: WebDriver):
        """ Initialize the TasksPage object
            Gets the instance parameter driver: WebDriver for Selenium automation"""

        self.driver = driver
        self.actions = ActionChains(driver)

    def create_new_task(self):
        """Clicks on the New Task button in the General task page and enters to the 'New Task' page"""

        create_new_task = self.driver.find_element(By.XPATH, "//span[text()='New Task...']")
        safe_click(create_new_task)

    def enter_manage_endpoints(self):
        """Clicks on the Manage endpoints button and enters to the 'Manage endpoints page'"""

        manage_endpoints = self.driver.find_element(By.XPATH, "//span[text()='Manage Endpoint Connections...']")
        safe_click(manage_endpoints)

    def import_task(self):
        """Clicks on the import task button, which provide an ability to import a task to Replicate with a Jason file"""

        import_task = self.driver.find_element(By.XPATH, "//span[text()='Import Task']")
        safe_click(import_task)

    def export_task(self):
        """Clicks on the export task button which creates a Jason file with the task definition"""

        export_task = self.driver.find_element(By.XPATH, "//span[text()='Export Task']")
        safe_click(export_task)

    def find_task_element(self, task_name: str):
        """ Find and return a task element on the page by its name.
            :param task_name: The name of the task to locate.
            :return: The WebElement representing the task element, or None if not found. """

        task = self.driver.find_element(By.XPATH, f"//*[@class='taskImageName ellipsisStyle ng-binding' and text()='{task_name}']")
        return task

    def select_task(self, task_name: str):
        """ Select a task on the page by its name.
            :param task_name: The name of the task to select. """

        task = self.find_task_element(task_name)
        safe_click(task)

    def open_task(self, task_name: str):
        """ Open a task by name on the page and enter to the tasks configurations and design.
            :param task_name: The name of the task to open. """

        self.select_task(task_name)
        open_task_option = self.driver.find_element(By.XPATH, "//span[text()='Open']")
        safe_click(open_task_option)

    def delete_task(self, task_name: str):
        """ Delete a task entirely from Qlik Replicate by name on the page.
            :param task_name: The name of the task to delete. """

        self.select_task(task_name)
        delete_task_button = self.driver.find_element(By.XPATH, "//span[text()='Delete...']")
        safe_click(delete_task_button)
        ok_button = self.driver.find_element(By.XPATH, "//button[text()='OK']")
        safe_click(ok_button)

    def double_click_task(self, task_name: str):
        """ Double-click on a task by its name and entering to the task page.
            :param task_name: The name of the task to double-click. """

        task = self.find_task_element(task_name)
        self.actions.double_click(task)

    def enter_view_logs(self):
        """Click on the View logs button and allowing to see the task log page"""

        view_logs = self.driver.find_element(By.XPATH, "//span[text()='View Logs...']")
        safe_click(view_logs)

    def download_logs(self):
        """Enter to the task log and download them to the machine"""

        self.enter_view_logs()
        download_logs_icon = self.driver.find_element(By.CSS_SELECTOR,
                                                      "[class='bootstrapGlyphicon glyphicon-download-alt']")
        safe_click(download_logs_icon)

    def run_task_dropdown(self, task_name: str):
        """ Open the task's Run options dropdown menu by clicking on it, as part of the task management.
            :param task_name: The name of the task for which to open the dropdown menu. """

        self.select_task(task_name)
        run_dropdown = self.driver.find_element(By.CSS_SELECTOR, "button[data-toggle='dropdown']>[class='caret']")
        safe_click(run_dropdown)

    def run_new_task(self, task_name: str):
        """ Start a new task.
            :param task_name: The name of the task for which to start process"""

        self.run_task_dropdown(task_name)
        start_processing = self.driver.find_element(By.XPATH,
                                                    "//li[@title='Start Processing']/a[text()='Start Processing']")
        safe_click(start_processing)

    def reload_task(self, task_name: str):
        """ Start a task again by reloading it.
            :param task_name: The name of the task for which to start process"""

        self.run_task_dropdown(task_name)
        reload_task = self.driver.find_element(By.XPATH,
                                                    "//*[@title='Reload Target...']/a[text()='Reload Target...']")
        safe_click(reload_task)
        yes_button = self.driver.find_element(By.XPATH,
                                                    "//button[text()='Yes']")
        safe_click(yes_button)

    def stop_task(self, task_name: str):
        """ Stop a task entirely.
            :param task_name: The name of the task for which to start process"""

        self.select_task(task_name)
        stop_task_element = self.driver.find_element(By.XPATH, "//span[text()='Stop']")
        safe_click(stop_task_element)
        yes_button = self.driver.find_element(By.XPATH, "//button[text()='Yes']")
        safe_click(yes_button)





