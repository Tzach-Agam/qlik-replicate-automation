from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from utilities.utility_functions import safe_click

class NewTaskPage:
    """ The NewTaskPage class represents the 'New Task' dialog box on Qlik Replicate. It appears after clicking on 'New
        Task' on the 'Tasks View', and it allows creation and configuration of a new task.
        This class provide methods for interacting with elements on the 'New Task 'dialog box, and allows to perform
        functionalities like creating and determine a task definition """

    def __init__(self, driver: WebDriver):
        """ Initialize the NewTaskPage object
            :param driver: WebDriver instance for Selenium automation. """
        self.driver = driver

    def enter_task_name(self, task_name: str):
        """ Generates a random task name and inputs it into the task name input field.
            :param task_name: The name of the task to be entered. """
        from random import randint
        random_number = randint(100000, 999999)
        full_task_name = f"{task_name}{random_number}"
        task_name_input = self.driver.find_element(By.XPATH, "//*[@id='Name']")
        task_name_input.clear()
        task_name_input.send_keys(full_task_name)
        return full_task_name

    def enter_description(self, description: str):
        """ Enter the task description into the input field.
            :param description: The description of the task to be entered. """
        description_input = self.driver.find_element(By.XPATH, "//*[@id='description']")
        description_input.send_keys(description)

    def close_new_task(self):
        """Click the 'OK' button to close the new task creation dialog."""
        close_button = self.driver.find_element(By.XPATH, "//*[text()='OK']")
        safe_click(close_button)

    def choose_store_changes(self):
        """ Click the 'Store Changes' toggle button. A task configured with 'Store Changes' will replicate to the target
            Database a table containing all the changes done to the tables that were replicated. """
        store_changes = self.driver.find_element(By.CSS_SELECTOR, "[class='StoreChanges toggleButton']")
        safe_click(store_changes)

    def fl_button(self):
        """ Click the 'Full Load' toggle button which is turned on by default. Will either remove the Full Load
            functionality (if it is already selected) or add it (if it's not selected)."""
        full_load = self.driver.find_element(By.CSS_SELECTOR, "[class='FullLoad toggleButton on']")
        safe_click(full_load)

    def cdc_button(self):
        """ Click the 'Apply Changes' toggle button which is turned on by default. Will either remove the CDC
            functionality (if it is already selected), or add it (if it's not selected)."""
        cdc = self.driver.find_element(By.CSS_SELECTOR, "[class='ApplyChanges toggleButton on']")
        safe_click(cdc)

    def new_task_creation(self, task_name: str):
        """ Create a new task by entering the task name and description and then closing the dialog. This method combines
            previous methods and allows creation of a task with default configurations.
            :param task_name: The name of the task to be entered."""
        new_task_name = self.enter_task_name(task_name)
        self.close_new_task()
        print(f"Task {new_task_name} created")
        return new_task_name
