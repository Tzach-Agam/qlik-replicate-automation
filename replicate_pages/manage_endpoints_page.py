from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from utilities.utility_functions import safe_click


class ManageEndpoints:
    """ The ManageEndpoints class represents the 'Manage Endpoints Connections' windows on Qlik Replicate. In it, you
        create, edit and configure source and target endpoints, which will be used for the replication task.
        The class provide methods that will interact with the 'Manage Endpoints Connections' window functionalities,like
        creation, deletion and configuration of an endpoint """

    def __init__(self, driver: WebDriver):
        """ Initialize the ManageEndpoints object
            :param driver: WebDriver instance for Selenium automation. """

        self.driver = driver
        self.actions = ActionChains(driver)

    def new_endpoint_connection(self):
        """Click the 'New Endpoint Connection' button for creation of a new endpoint."""

        new_endpoint = self.driver.find_element(By.XPATH, "//*[text()='New Endpoint Connection']")
        safe_click(new_endpoint)

    def enter_endpoint_name(self, name: str):
        """ Enter the endpoint name.
            :param name: The name of the endpoint. """

        endpoint_name = self.driver.find_element(By.XPATH, "//*[@id='endpointName']")
        endpoint_name.clear()
        endpoint_name.send_keys(name)

    def enter_endpoint_description(self, description: str):
        """ Enter description for the endpoint.
            :param description: The description of the endpoint. """

        endpoint_desc = self.driver.find_element(By.CSS_SELECTOR, "[name='endpointDescription']")
        endpoint_desc.send_keys(description)

    def choose_target_role(self):
        """ Choose 'target' as the role of the endpoint. The endpoint will receive the data in the replication task. """

        target_role = self.driver.find_element(By.CSS_SELECTOR, "[id='targetRB']>[class='radioBtnSpan ']")
        safe_click(target_role)

    def choose_endpoint_type(self, endpoint_type: str):
        """ Choose the type of the endpoint, meaning the database that the endpoint represents, for example Oracle, MySQL,
            PostgresSQL etc. """

        input_element = self.driver.find_element(By.CSS_SELECTOR, "[class='textInputInRowWrap']>[type='text']")
        input_element.send_keys(endpoint_type)
        endpoint_mapping = {
            "sql server": "Microsoft SQL Server",
            "oracle": "Oracle",
            "mySQL": "MySQL",
            "postgresql": "PostgreSQL"
        }
        if endpoint_type.lower() in endpoint_mapping:
            endpoint_text = endpoint_mapping[endpoint_type.lower()]
            endpoint_to_select = self.driver.find_element(By.XPATH, f"//*[text()='{endpoint_text}']")
            safe_click(endpoint_to_select)
        else:
            raise ValueError("Invalid endpoint type")

    def sql_auth_option(self):
        """ Choose 'SQL Server authentication' for when the endpoint created is SQL Server"""

        sql_auth_checkbox = self.driver.find_element(By.XPATH, "//*[@id='sqlAuth']/span[1]")
        safe_click(sql_auth_checkbox)

    def enter_server(self, server):
        """ Enter the server information for the database.
            :param server: The server information to be entered. """

        server_element = self.driver.find_element(By.CSS_SELECTOR, "[input-id='server']")
        server_element.send_keys(server)

    def enter_username(self, username):
        """ Enter the username for database authentication.
            :param username: The username to be entered. """

        username_element = self.driver.find_element(By.CSS_SELECTOR, "[name='username']")
        username_element.send_keys(username)

    def enter_password(self, password):
        """ Enter the password for database authentication.
            :param password: The password to be entered. """

        password_element = self.driver.find_element(By.CSS_SELECTOR, "[name='password']")
        password_element.send_keys(password)

    def enter_database(self, database):
        """ Enter the database name.
            :param database: The database name or identifier to be entered. """

        database_element = self.driver.find_element(By.CSS_SELECTOR, "[name='database']")
        database_element.send_keys(database)

    def test_connection(self):
        """Click the 'Test Connection' button to test the database connection."""

        test_connection_element = self.driver.find_element(By.XPATH, "//*[text()='Test Connection']")
        safe_click(test_connection_element)

    def save(self):
        """Click the 'Save' button to save the endpoint configuration."""

        save_button = self.driver.find_element(By.XPATH, "//*[text()='Save']")
        safe_click(save_button)

    def close(self):
        """Click the 'Close' button to close the endpoint configuration dialog."""

        close_button = self.driver.find_element(By.XPATH, "//*[text()='Close']")
        safe_click(close_button)

    def close_and_save(self):
        """ Close the endpoint configuration dialog and save changes if prompted.
            This function closes the dialog and, if changes are made, clicks the 'Save' button. """

        self.close()
        save_button = self.driver.find_element((By.XPATH, "//div[@class='modal-footer ng-scope']/*[text()='Save']"))
        safe_click(save_button)

    def close_no_save(self):
        """ Close the endpoint configuration dialog without saving changes.
            This function closes the dialog without saving any changes. """

        self.close()
        no_save_button = self.driver.find_element(
            (By.XPATH, "//div[@class='modal-footer ng-scope']/*[text()='Don't save']"))
        safe_click(no_save_button)

    def close_and_cancel(self):
        """ Close the endpoint configuration dialog and cancel any changes.
            This function closes the dialog and cancels any unsaved changes. """

        self.close()
        cancel_button = self.driver.find_element((By.XPATH, "//div[@class='modal-footer ng-scope']/*[text()='Cancel']"))
        safe_click(cancel_button)

    def delete_endpoint(self, endpoint_data: dict):
        """ Delete an endpoint entirely using the endpoint data from a dictionary in the endpoint configuration module.
            :param endpoint_data: Information about the endpoint to be deleted. """

        endpoint = self.driver.find_element(By.XPATH, f"//*[text()='{endpoint_data['name']}']")
        safe_click(endpoint)
        delete_button = self.driver.find_element(By.XPATH, "//*[text()='Delete']")
        safe_click(delete_button)
        ok_button = self.driver.find_element(By.XPATH, "//button[text()='OK']")
        safe_click(ok_button)

    def delete_endpoint_2(self, endpoint_name):
        """ Delete an endpoint entirely using a different method.
            :param endpoint_name: The name of the endpoint to be deleted. """

        endpoint = self.driver.find_element(By.XPATH, f"//*[text()='{endpoint_name}']")
        self.actions.context_click(endpoint).perform()
        self.driver.find_element(By.XPATH, "//*[@id='5']").click()
        ok_button = self.driver.find_element(By.XPATH, "//button[text()='OK']")
        safe_click(ok_button)

    def create_rdbms_endpoint(self, endpoint_data: dict):
        """ Create an RDBMS endpoint connection with the provided endpoint data. All the endpoint information, including
            its name, type, role and connection credentials, will be taken from a specific dictionary with the endpoint's
            information in the endpoints configuration module.
            :param endpoint_data: Information about the RDBMS endpoint to be created. """

        self.new_endpoint_connection()
        self.enter_endpoint_name(endpoint_data["name"])
        self.enter_endpoint_description(endpoint_data["description"])

        if endpoint_data["role"].lower() == "target":
            self.choose_target_role()

        self.choose_endpoint_type(endpoint_data["type"])

        if endpoint_data["type"] == "SQL Server":
            self.sql_auth_option()

        self.enter_server(endpoint_data["server"])
        self.enter_username(endpoint_data["username"])
        self.enter_password(endpoint_data["password"])

        if endpoint_data["type"] == "SQL Server" or endpoint_data["type"] == "PostgresSQL":
            self.enter_database(endpoint_data["database"])

        self.save()
