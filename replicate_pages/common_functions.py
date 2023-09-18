from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utilities.utility_functions import safe_click
from selenium.common.exceptions import TimeoutException

class CommonFunctions:
    """ The Common_functions class provides a set of common utility functions that can be used across various pages and
        actions in Qlik Replicate. These functions include handling secure browser connections, waiting for loader icons
        to complete, and navigating to different pages."""

    def __init__(self, driver: WebDriver):
        """ Initialize the CommonFunctions object
            :param driver: WebDriver instance for Selenium automation. """

        self.driver = driver
        self.actions = ActionChains(driver)

    def secure_browser_connection(self):
        """ Secure the browser connection. This method clicks on the 'Advanced' button and then the 'Proceed' button to
            handle secure browser connection warnings."""

        advanced_button = self.driver.find_element(By.ID, "details-button")
        safe_click(advanced_button)
        proceed_button = self.driver.find_element(By.ID, "proceed-link")
        safe_click(proceed_button)

    def loader_icon_opening_replicate(self):
        """ Wait for loader icon to complete when opening Qlik Replicate. This is useful for waiting until Replicate has
            finished loading. """

        try:
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "[class='connectedLoader loader'][loader-type='ball-spin-fade-loader']")))
            wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "[class='connectedLoader loader'][loader-type='ball-spin-fade-loader']")))
        except TimeoutException:
            pass

    def task_data_loader(self):
        """ Wait until the task data loader disappears. Used to wait for task-related data loading to complete."""

        try:
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "[class='loader'][loader-type='ball-clip-rotate-multiple']")))
            wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "[class='loader'][loader-type='ball-clip-rotate-multiple']")))
        except TimeoutException:
            pass

    def navigate_to_main_page(self, target_page):
        """ Navigate to the different general pages in Qlik Replicate.
            This method opens the dropdown menu and selects the target page based on the provided 'target_page' argument,
            which can be "tasks" for 'Task View' or "server" for the 'Server page'.
            :param target_page : The name of the target page to navigate to (e.g., "tasks" or "server"). """

        dropdown = self.driver.find_element(By.CSS_SELECTOR, ".dropdown-toggle.hiddenActionButton.right")
        safe_click(dropdown)

        if target_page == "tasks":
            page_link_text = "Tasks"
        elif target_page == "server":
            page_link_text = "Server"
        else:
            raise ValueError("Invalid target page name")

        page_link = self.driver.find_element(By.XPATH, f"//a[text()='{page_link_text}']")
        safe_click(page_link)


