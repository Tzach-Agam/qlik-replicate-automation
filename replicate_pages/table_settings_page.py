from time import sleep

from selenium.common import StaleElementReferenceException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
        transform_sec = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Transform']")))
        safe_click(transform_sec)
        self.wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Input']")))
        self.wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Output']")))

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
        add_column_element = self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//*[text()='Add Column']")))
        safe_click(add_column_element)
        self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[ng-model='rowData.newColumnName']")))

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
        remove_col_element = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[ng-click='removeSingleColumn()']")))
        safe_click(remove_col_element)

    def add_column_expression(self, column_name:str, expression:str):
        """Adds expression to the target table"""
        self.add_column()
        self.enter_new_column_name(column_name)
        self.enter_expression_builder(column_name)
        columns_title = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[title='Columns']>div>a>span")))
        columns_title.click()
        editor_container = self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".CodeMirror")))
        # Inject the expression with CodeMirror's API
        self.driver.execute_script(
            "arguments[0].CodeMirror.setValue(arguments[1]);",
            editor_container,
            expression
        )
        #expression_builder_input.send_keys(expression)
        try:
            ok_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='OK']")))
            safe_click(ok_button)
        except (StaleElementReferenceException, ElementNotInteractableException):
            ok_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='OK']")))
            safe_click(ok_button)

    def filter_section(self):
        """Clicks on General section"""
        transform_sec = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Filter']")))
        safe_click(transform_sec)
        self.wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Data Columns']")))
        self.wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Filter Conditions']")))

    def filter_select_column(self, column_name: str):
        """Selects input column"""
        column_element = self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(), '{column_name}')]")))
        safe_click(column_element)
        add_column = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@ng-click='onAddToFilter()']")))
        safe_click(add_column)

    def add_filter_less_or_equal(self,column_name: str, value: str):
        self.filter_select_column(column_name)
        ranges_element = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//tr[@sly-repeat='rowData in renderedRows ']/descendant::button[@class='attBtn glyphButton text-right ']/span[1]")))
        safe_click(ranges_element)
        self.wait.until(EC.visibility_of_element_located((By.XPATH, f"//*[text()='{column_name} INCLUDE Ranges']")))
        add_range = self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//*[text()='Add Range']")))
        safe_click(add_range)
        less_equal_element = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Less than or equal to']")))
        safe_click(less_equal_element)
        self.wait.until(EC.visibility_of_element_located((By.XPATH, f"//div[@title='Delete']")))
        filter_value = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//link-text-input[@editable='true']"))
        )
        filter_value.click()
        self.driver.execute_script(
            """
            const elem = arguments[0];
            const value = arguments[1];
            elem.focus();
            elem.innerText = value;
            elem.dispatchEvent(new Event('input', { bubbles: true }));
            elem.dispatchEvent(new Event('change', { bubbles: true }));
            elem.dispatchEvent(new KeyboardEvent('keydown', {key: 'Enter', bubbles: true}));
            elem.dispatchEvent(new KeyboardEvent('keyup', {key: 'Enter', bubbles: true}));
            """,
            filter_value, value
        )
        filter_value.click()
        sleep(10)
        try:
            ok_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='OK'][1]")))
            safe_click(ok_button)
        except (StaleElementReferenceException, ElementNotInteractableException):
            ok_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='OK'][1]")))
            safe_click(ok_button)
        sleep(5)

    def ok_button(self):
        """Clicks on the OK button"""
        try:
            ok_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='OK']")))
            safe_click(ok_button)
        except (StaleElementReferenceException, ElementNotInteractableException):
            ok_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='OK']")))
            safe_click(ok_button)









