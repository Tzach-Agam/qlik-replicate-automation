from time import sleep

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from configurations.config_manager import ConfigurationManager
from utilities.utility_functions import safe_click

class TableSettings:
    """ The TableSettings class represents the 'Table Settings' dialog box on Qlik Replicate. In it, you configure
        task-specific replication settings.
        The class will provide methods for the task settings page functionality, like setting target replication schemas
        or adding logs"""

    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(self.driver, 20)
        self.actions = ActionChains(self.driver)

    def general_section(self):
        """Clicks on General section"""
        general_sec = self.driver.find_element(By.XPATH, "//*[text()='General']")
        safe_click(general_sec)

    def enter_table_schema(self, schema_name):
        """Enters schema that the table will be replicated to"""
        table_schema = self.driver.find_element(By.XPATH, "//*[@id='Name']")
        table_schema.send_keys(schema_name)

    def enter_table_name(self, table_name):
        """Enters table name > the table will be created in the target with that name"""
        table_name = self.driver.find_element(By.XPATH, "//*[@id='TableName']")
        table_name.send_keys(table_name)

    def transform_section(self):
        """Clicks on General section"""
        transform_sec = self.driver.find_element(By.XPATH, "//*[text()='Transform']")
        safe_click(transform_sec)

    def select_input_column(self, column_name:str):
        """Selects input column"""
        column_element = self.driver.find_element(By.CSS_SELECTOR, f"[strict-text='{column_name}']")
        safe_click(column_element)

    def select_output_column(self, column_name):
        """Selects input column"""
        column_element = self.driver.find_element(By.XPATH, f"//*[text()='{column_name}']")
        safe_click(column_element)

    def add_column(self):
        """Adds column"""
        add_column_element = self.driver.find_element(By.XPATH, f"//*[text()='Add Column']")
        safe_click(add_column_element)

    def enter_new_column_name(self, column_name:str):
        new_col_name = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[ng-model='rowData.newColumnName']")))
        new_col_name.send_keys(column_name)
        self.transform_section()
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='OK']")))

    def enter_expression_builder(self, column_name):
        """Clicks on expression builder element"""
        function_element = self.wait.until(EC.element_to_be_clickable((By.XPATH,
                                                    f"//span[text()='{column_name}']/ancestor::tr/descendant::span[@class='att-glyph icon-gl-expression-builder']")))
        safe_click(function_element)
        self.wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Expression Builder']")))

    def remove_column(self,column_name):
        """Removes a column"""
        self.select_output_column(column_name)
        remove_col_element = self.driver.find_element(By.CSS_SELECTOR, "[ng-click='removeSingleColumn()']")
        safe_click(remove_col_element)

    def add_column_expression(self, column_name:str, expression:str):
        """Adds expression to the target table"""
        self.add_column()
        self.enter_new_column_name(column_name)
        self.enter_expression_builder(column_name)
        columns_title = self.driver.find_element(By.CSS_SELECTOR, "[title='Columns']>div>a>span")
        columns_title.click()
        editor_container = self.driver.find_element(By.CSS_SELECTOR, ".CodeMirror")

        # Inject the expression with CodeMirror's API
        self.driver.execute_script(
            "arguments[0].CodeMirror.setValue(arguments[1]);",
            editor_container,
            expression
        )

        #expression_builder_input.send_keys(expression)
        ok_button = self.driver.find_element(By.XPATH, "//*[text()='OK']")
        safe_click(ok_button)
        self.wait.until(EC.visibility_of_element_located((By.XPATH, "//*[@id='att-modal-div']/div/div/div[1]/span")))

    def add_header(self, column_name:str, header_name:str):
        """Adds header to the target table
        :param column_name: The name of the header column
        :param header_name: The name of the header"""
        self.add_column()
        self.enter_expression_builder(column_name)
        headers_title = self.driver.find_element(By.CSS_SELECTOR, "[title='Headers']>div>a>span")
        headers_title.click()
        header_to_choose = self.driver.find_element(By.XPATH, f"//div[text()='{header_name}']")
        safe_click(header_to_choose)
        self.actions.double_click(header_to_choose)
        ok_button = self.driver.find_element(By.XPATH, "//*[text()='OK']")
        safe_click(ok_button)
        self.enter_new_column_name(column_name)

    def ok_button(self):
        """Clicks on the OK button"""
        ok_button = self.driver.find_element(By.XPATH, "//*[text()='OK']")
        safe_click(ok_button)









