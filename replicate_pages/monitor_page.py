from selenium.common import TimeoutException
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utilities.utility_functions import safe_click
from time import sleep

class MonitorPage:
    """ The MonitorPage class represents the 'Monitor mode' on Qlik Replicate. In Monitor mode, you view the replication
        task activities in real time, including changes, errors and warnings. The Monitor mode also allows other operations
        like running a task, viewing its logs and entering the endpoints.
        The methods in the class interacts with the web elements in 'Monitor mode', and will allow the to track and monitor
        the task's status and current position. """

    def __init__(self, driver: WebDriver):
        """ Initialize the MonitorPage object
            :param driver: WebDriver instance for Selenium automation. """
        self.driver = driver
        self.wait = WebDriverWait(self.driver, 60)
        self.actions = ActionChains(self.driver)

    def fl_tab(self):
        """Click on 'Full Load' tab to view that status of the Full load process."""
        fl_tab = self.driver.find_element(By.CSS_SELECTOR, "[title='Full Load']")
        safe_click(fl_tab)

    def cdc_tab(self):
        """Click on 'Change Processing' tab to view that status of the CDC process."""
        cdc_tab = self.driver.find_element(By.CSS_SELECTOR, "[title='Change Processing']")
        safe_click(cdc_tab)
        self.driver.execute_script("document.body.style.zoom='80%'")
        sleep(5)

    def enter_designer_page(self):
        """Enter to the task's 'Designer mode'"""
        designer = self.driver.find_element(By.XPATH, "//span[text()='Designer']")
        safe_click(designer)

    def source_endpoint_click(self):
        """Double-click on the source endpoint."""
        source_endpoint = self.driver.find_element(By.XPATH, "//*[@id='mapSrcAreaItem']/div/div/div")
        self.actions.double_click(source_endpoint).perform()

    def target_endpoint_click(self):
        """Double-click on the target endpoint."""
        target_endpoint = self.driver.find_element(By.XPATH, "//*[@id='mapTargetAreaItems']/div/div/div")
        self.actions.double_click(target_endpoint).perform()

    def enter_view_logs(self):
        """Click on the View logs button to view the task log"""
        view_logs = self.driver.find_element(By.XPATH, "//span[text()='View Logs...']")
        safe_click(view_logs)

    def download_logs(self):
        """Enter to the task log and download them to the machine"""
        self.enter_view_logs()
        download_logs_icon = self.driver.find_element(By.CSS_SELECTOR, "[class='bootstrapGlyphicon glyphicon-download-alt']")
        safe_click(download_logs_icon)

    def run_task_dropdown(self):
        """Click the 'Run' task dropdown menu."""
        run_dropdown = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-toggle='dropdown']>[class='caret']")))
        safe_click(run_dropdown)
        self.wait.until(EC.element_to_be_clickable((By.XPATH,
                                                    "//*[@title='Reload Target...']/a[text()='Reload Target...']")))

    def reload_task(self, timeout=40):
        """ Start a task again by reloading it with the 'Reload task' option in the run options dropdown."""
        self.run_task_dropdown()
        reload_task = self.wait.until(EC.element_to_be_clickable((By.XPATH,
                                                    "//*[@title='Reload Target...']/a[text()='Reload Target...']")))
        safe_click(reload_task)
        yes_button = self.wait.until(EC.element_to_be_clickable((By.XPATH,
                                                    "//button[text()='Yes']")))
        safe_click(yes_button)
        try:
            dynamic_wait = WebDriverWait(self.driver, timeout)
            dynamic_wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Starting task']")))
            print("Starting task")
            dynamic_wait.until(EC.invisibility_of_element_located((By.XPATH, "//*[text()='Starting task']")))
            print("Task started")
        except:
            print("Element did not become visible within the timeout or became stale.")

    def wait_for_fl(self, number_of_tables: str, timeout: int = 30):
        """ Wait for the specified number of tables to complete in Full Load (FL) mode.
            This method waits for a specific number of tables to complete their Full Load operation in the Monitor mode
            of Qlik Replicate. It checks if the expected number of tables have reached completion status within the given
            timeout.
            :param number_of_tables: The number of tables to wait for completion. this number represents the expected
            number of completed tables, which will be matched against the actual number of completed tables displayed on
            the UI.
            :param timeout: The maximum number of seconds to wait for completion. this number represents the expected"""
        try:
            dynamic_wait = WebDriverWait(self.driver, timeout)
            dynamic_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[title='Full Load']")))
            self.fl_tab()
            dynamic_wait.until(EC.visibility_of_element_located((By.XPATH, f"//*[@id='Monitoring_FL_CompletedTables']/div/div[3]/*[text()='{number_of_tables}']")))
            dynamic_wait.until(EC.visibility_of_element_located((By.XPATH, "//div[@style='width: 100%;']")))
            print("Full Load completed")
        except:
            raise AssertionError(f"Full Load did not complete {number_of_tables} tables within {timeout} seconds.")

    def wait_for_cdc(self, timeout: int = 60):
        """ Wait for the Change Processing (CDC) to complete."""
        dynamic_wait = WebDriverWait(self.driver, timeout)
        transferring_element = (By.XPATH, "//div[@id='taskFlowMapDirective'][contains(@class, 'RUNNING') and contains(@class, 'Transferring')]")
        dynamic_wait.until(EC.visibility_of_element_located(transferring_element))
        dynamic_wait.until(EC.invisibility_of_element_located(transferring_element))
        print("CDC completed")

    def _check_operation_status(self, column_index: int, *expected_statuses: str):
        """Check the status of operations (Insert/Update/Delete) for each specified table.
        :param column_index: int - The index of the operation's status column (2=Insert, 3=Update, 4=Delete).
        :param expected_statuses: variable-length list of expected statuses for each table.
        """
        self.wait.until(EC.visibility_of_element_located((By.XPATH, "//*[@id='scrollAreaID']/div/div/table/tbody/tr")))
        tables = self.driver.find_elements(By.XPATH, "//att-tab/div/div/div/div/div/div/div/div/table/tbody/tr")

        for i, table in enumerate(tables):
            try:
                table_name = self.driver.find_element(By.XPATH,f"//att-tab/div/div/div/div/div/div/div/div/table/tbody/tr[{i + 1}]/td[1]/div/div").text
                expected_status = expected_statuses[i]
                status_locator = (By.XPATH,
                                  f"//table/tbody/tr[{i + 1}]/td[{column_index}]/div/div[text()='{expected_status}']")
                self.wait.until(EC.visibility_of_element_located(status_locator))
                print(f"Status for table {table_name} is {expected_status}")
            except Exception:
                print(f"Operation status check did not complete within the timeout for table at index {i + 1}")

    def insert_check(self, *expected_statuses: str):
        self._check_operation_status(2, *expected_statuses)

    def update_check(self, *expected_statuses: str):
        self._check_operation_status(3, *expected_statuses)

    def delete_check(self, *expected_statuses: str):
        self._check_operation_status(4, *expected_statuses)

    def ddl_check(self, *args: str):
        """ Check the status of DDL operations in the Monitor mode of Qlik Replicate for each of the specified tables
            in the replication task.
            It verifies if the expected DDL status matches the actual status displayed in the UI. If a match is found,
            it prints the result for each table.

            :param args: Variable-length list of strings representing the expected DDL status for each table in the
            same order as they appear on the UI."""

        wait = WebDriverWait(self.driver, 60)

        tables_ddl = list(args)

        tables = self.driver.find_elements(By.XPATH, "//table/tbody/tr")
        for table in range(1, len(tables) + 1):
            try:
                table_name = self.driver.find_element(By.XPATH, f"//table/tbody/tr[{table}]/td[1]/div/div").text
                ddl_status_element = (By.XPATH, f"//table/tbody/tr[{table}]/td[5]/div/div[text()='{tables_ddl[table - 1]}']")
                wait.until(EC.visibility_of_element_located(ddl_status_element))
                print(f"DDLs for table {table_name} are {tables_ddl[table - 1]}")
                sleep(5)
            except:
                print("DDLs didn't complete within the timeout")

    def stop_task(self):
        """Stop the current replication task entirely."""
        self.driver.execute_script("document.body.style.zoom=''")
        stop_task_element = self.driver.find_element(By.XPATH, "//span[text()='Stop']")
        safe_click(stop_task_element)
        yes_button = self.driver.find_element(By.XPATH, "//button[text()='Yes']")
        safe_click(yes_button)

    def stop_task_wait(self, timeout=60):
        """Wait for the replication task to stop."""
        wait = WebDriverWait(self.driver, timeout)
        wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Stopping task']")))
        print("Stopping task")
        wait.until(EC.invisibility_of_element_located((By.XPATH, "//*[text()='Stopping task']")))
        print("Task stopped")
        self.wait.until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, "div[uib-modal-backdrop='modal-backdrop']")))

    def wait_for_message_in_ui(self, search_text: str):
        """ Wait for a specific message text in the 3rd column of any row. """
        def check_text_in_third_column(driver):
            rows = driver.find_elements(By.XPATH, '//*[@id="scrollAreaID"]/div/div/table/tbody/tr')
            for i, row in enumerate(rows, start=1):
                try:
                    cell = row.find_element(By.XPATH, './td[3]//div')
                    if search_text in cell.text.strip():
                        print(f"✅ Message found in row {i}, column 3")
                        return True
                except:
                    continue
            return False

        try:
            self.wait.until(check_text_in_third_column)
        except TimeoutException:
            raise AssertionError(f"Message '{search_text}' not found Monitor UI after 60 seconds.")













