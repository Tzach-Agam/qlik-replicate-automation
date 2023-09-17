from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from utilities.utility_functions import safe_click


class TableSelection:
    """ The TableSelection class represents the 'Table Selection' windows on Qlik Replicate. In it, you choose the schemas
        and tables that will be replicated during the replication task from the source to the target.
        The methods in the class will interact with the elements on the 'Table Selection' window and wil allow to choose
        the schemas in tables for the automated tasks."""

    def __init__(self, driver: WebDriver):
        """ Initialize the TableSelection object
            :param driver: WebDriver instance for Selenium automation. """

        self.driver = driver

    def choose_schema(self, source_schema):
        """Enter the name of source schema for the replication task """

        schema_input = self.driver.find_element(By.CSS_SELECTOR, "[place-holder='Select a Schema in the list or search']>div>input")
        schema_input.send_keys(source_schema)
        schema_input.send_keys(Keys.ENTER)

    def choose_table(self, source_table):
        """ Enter the name of the table that is wished to be replicated """

        table_input = self.driver.find_element(By.CSS_SELECTOR, "[class='controlsValues']>input")
        table_input.send_keys(source_table)
        table_input.send_keys(Keys.ENTER)

    def include_schema_button(self):
        """ Click on the 'Include' button to set the chosen schema in the 'Table Selection Patterns'.
            By doing so the all the tables under that schema will be replicated to the target """

        include_button = self.driver.find_element(By.XPATH, "//*[text()='Include']")
        safe_click(include_button)

    def remove_schema_button(self):
        """ Click on the 'Re,ove' button to remove a schema from 'Table Selection Patterns'.
            By doing so the schema and the tables under it won't be part of the replication task. """

        remove_button = self.driver.find_element(By.XPATH, "//*[text()='Remove']")
        safe_click(remove_button)

    def search_for_tables(self):
        """Click on the 'Search' button to search for tables under a chosen schema"""

        search_button = self.driver.find_element(By.XPATH, "//*[text()='Search']")
        safe_click(search_button)

    def select_one_table(self):
        """Select one table of the tables available under a schema"""

        one_table_button = self.driver.find_element((By.CSS_SELECTOR, "[class='bottomButtons']>button:nth-child(1)"))
        safe_click(one_table_button)

    def select_all_tables(self):
        """Select all the tables of the tables available under a schema"""

        all_tables_button = self.driver.find_element((By.CSS_SELECTOR, "[class='bottomButtons']>button:nth-child(3)"))
        safe_click(all_tables_button)

    def ok_button_click(self):
        """Click on the 'OK' button to save the changes and configurations"""

        ok_button = self.driver.find_element(By.XPATH, "//*[text()='OK']")
        safe_click(ok_button)

    def cancel_button_click(self):
        """Click on the 'Cancel' button to leave the 'Table Selection' window without saving the configurations """

        cancel_button = self.driver.find_element(By.XPATH, "//*[text()='Cancel']")
        safe_click(cancel_button)

    def choose_source_schema(self, source_schema: str):
        """ Choose a source schema and include it. This method combines previous methods in order to set default settings
            for the automated replication task. It will select a source schema, include it, and confirm the selection by
            clicking 'OK'.
            :param source_schema: The name of the source schema to be selected and included. """

        self.choose_schema(source_schema)
        self.include_schema_button()
        self.ok_button_click()



