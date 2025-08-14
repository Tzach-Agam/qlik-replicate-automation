from selenium.common import StaleElementReferenceException, ElementNotInteractableException
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from utilities.utility_functions import safe_click
from configurations.config_manager import ConfigurationManager
import random


class ManageEndpoints:
    """ The ManageEndpoints class represents the 'Manage Endpoints Connections' windows on Qlik Replicate. In it, you
        create, edit and configure source and target endpoints, which will be used for the replication task.
        The class provide methods that will interact with the 'Manage Endpoints Connections' window functionalities,like
        creation, deletion and configuration of an endpoint """

    def __init__(self, driver: WebDriver, config: ConfigurationManager):
        """ Initialize the ManageEndpoints object
            :param driver: WebDriver instance for Selenium automation. """
        self.driver = driver
        self.wait = WebDriverWait(self.driver, 20)
        self.actions = ActionChains(self.driver)
        self.config = config

    def new_endpoint_connection(self):
        """Click the 'New Endpoint Connection' button to create a new endpoint."""
        new_endpoint = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button/span[text()='New Endpoint Connection']"))
        )
        safe_click(new_endpoint)
        self.wait.until(EC.visibility_of_element_located((By.XPATH, "//*[@id='endpointName']")))
        self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "[name='endpointDescription']")))
        self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "[class='textInputInRowWrap']>[type='text']")))

    def random_endpoint_name(self, config_section):
        """Creates a random name of the endpoint by appending a 6-digit random number.
        :param config_section: The base name of the endpoint. """
        random_number = random.randint(100000, 999999)  # 6-digit number
        return f"{self.config.get_section(config_section)['endpoint']}{random_number}"

    def enter_endpoint_name(self, name: str):
        """Enter the name of the endpoint."""
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
        try:
            target_role = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='targetRB']/span[1]")))
            safe_click(target_role)
        except (StaleElementReferenceException, ElementNotInteractableException):
            target_role = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='targetRB']/span[1]")))
            safe_click(target_role)

    def choose_endpoint_type(self, endpoint_type: str):
        """ Choose the type of the endpoint, meaning the database that the endpoint represents, for example Oracle, MySQL,
            PostgresSQL etc. """
        try:
            input_element = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[class='textInputInRowWrap']>[type='text']")))
            input_element.send_keys(endpoint_type)
        except (StaleElementReferenceException, ElementNotInteractableException):
            input_element = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[class='textInputInRowWrap']>[type='text']")))
            input_element.send_keys(endpoint_type)



    """------------------ SQL Server endpoint: Specific Methods ------------------"""

    def choose_sql_server_type(self):
        self.choose_endpoint_type("Microsoft SQL Server")
        sql_server_type = self.driver.find_element(By.XPATH, "//li//*[text()='Microsoft SQL Server']")
        safe_click(sql_server_type)
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='server']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Save']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Test Connection']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Advanced']")))

    def enter_sql_server(self, name: str):
        server = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='server']")))
        server.send_keys(name)

    def sql_auth_option(self):
        sql_auth_checkbox = self.driver.find_element(By.XPATH, "//*[@id='sqlAuth']/span[1]")
        safe_click(sql_auth_checkbox)

    def enter_sql_username(self, username):
        username_element = self.driver.find_element(By.CSS_SELECTOR, "[name='username']")
        username_element.send_keys(username)

    def enter_sql_password(self, password):
        password_element = self.driver.find_element(By.CSS_SELECTOR, "[name='password']")
        password_element.send_keys(password)

    def enter_sql_database(self, database):
        database_element = self.driver.find_element(By.CSS_SELECTOR, "[name='database']")
        database_element.send_keys(database)

    def create_sql_server_source_endpoint(self, endpoint_name):
        """Creates a SQL Server source endpoint using the parameters in the config.ini file
        :param endpoint_name: The name of the endpoint. """
        self.new_endpoint_connection()
        self.choose_sql_server_type()
        self.sql_auth_option()
        self.enter_sql_server(self.config.get_section('MSSQL_DB')['server'])
        self.enter_sql_username(self.config.get_section('MSSQL_DB')['username'])
        self.enter_sql_password(self.config.get_section('MSSQL_DB')['password'])
        self.enter_sql_database(self.config.get_section('MSSQL_DB')['database'])
        self.enter_endpoint_description('SQL Server Source Endpoint')
        self.enter_endpoint_name(endpoint_name)
        self.test_connection_valid()
        self.save()
        print("Created a SQL Server source endpoint:", endpoint_name)

    def create_sql_server_target_endpoint(self, endpoint_name):
        """Creates a SQL Server source endpoint using the parameters in the config.ini file
        :param endpoint_name: The name of the endpoint"""
        self.new_endpoint_connection()
        self.choose_target_role()
        self.choose_sql_server_type()
        self.sql_auth_option()
        self.enter_sql_server(self.config.get_section('MSSQL_DB')['server'])
        self.enter_sql_username(self.config.get_section('MSSQL_DB')['username'])
        self.enter_sql_password(self.config.get_section('MSSQL_DB')['password'])
        self.enter_sql_database(self.config.get_section('MSSQL_DB')['database'])
        self.enter_endpoint_description('SQL Server Target Endpoint')
        self.enter_endpoint_name(endpoint_name)
        self.test_connection_valid()
        self.save()
        print("Created a SQL Server target endpoint:", endpoint_name)



    """------------------ Oracle endpoint: Specific Methods ------------------"""

    def choose_oracle_type(self):
        self.choose_endpoint_type("Oracle")
        oracle_type = self.driver.find_element(By.XPATH, "//li//*[text()='Oracle']")
        safe_click(oracle_type)
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='server']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='username']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='password']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Save']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Test Connection']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Advanced']")))

    def enter_oracle_server(self, name: str):
        server = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='server']")))
        server.send_keys(name)

    def enter_oracle_username(self, username):
        username_element = self.driver.find_element(By.XPATH, "//*[@id='username']")
        username_element.send_keys(username)

    def enter_oracle_password(self, password):
        password_element = self.driver.find_element(By.XPATH, "//*[@id='password']")
        password_element.send_keys(password)
        self.driver.find_element(By.TAG_NAME, "body").click()

    def create_oracle_source_endpoint(self, endpoint_name):
        """Creates an Oracle source endpoint using the parameters in the config.ini file
        :param endpoint_name: The name of the endpoint. """
        self.new_endpoint_connection()
        self.choose_oracle_type()
        self.enter_oracle_server(self.config.get_section('Oracle_DB')['dsn'])
        self.enter_oracle_username(self.config.get_section('Oracle_DB')['username'])
        self.enter_oracle_password(self.config.get_section('Oracle_DB')['password'])
        self.enter_endpoint_description('Oracle Source Endpoint')
        self.enter_endpoint_name(endpoint_name)
        self.test_connection_valid()
        self.save()
        print("Created an Oracle source endpoint:", endpoint_name)

    def create_oracle_target_endpoint(self, endpoint_name):
        """Creates an Oracle target endpoint using the parameters in the config.ini file
        :param endpoint_name: The name of the endpoint. """
        self.new_endpoint_connection()
        self.choose_target_role()
        self.choose_oracle_type()
        self.enter_oracle_server(self.config.get_section('Oracle_DB')['dsn'])
        self.enter_oracle_username(self.config.get_section('Oracle_DB')['username'])
        self.enter_oracle_password(self.config.get_section('Oracle_DB')['password'])
        self.enter_endpoint_description('Oracle Target Endpoint')
        self.enter_endpoint_name(endpoint_name)
        self.test_connection_valid()
        self.save()
        print("Created an Oracle target endpoint:", endpoint_name)



    """------------------ Snowflake endpoint: Specific Methods ------------------"""

    def choose_snowflake_type(self):
        self.choose_endpoint_type("Snowflake")
        sql_server_type = self.driver.find_element(By.XPATH, "//li//*[text()='Snowflake']")
        safe_click(sql_server_type)
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='server']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Save']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Test Connection']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Advanced']")))

    def enter_snowflake_server(self, name: str):
        server = self.driver.find_element(By.XPATH, "//*[@id='server']")
        server.send_keys(name)

    def choose_snowflake_user_pass_auth(self):
        auth_options = self.driver.find_element(By.XPATH, "//*[text()='Key Pair']/following-sibling::span")
        safe_click(auth_options)
        user_pass_option = self.driver.find_element(By.XPATH, "//*[text()='Username and password']")
        safe_click(user_pass_option)

    def enter_snowflake_username(self, username):
        username_element = self.driver.find_element(By.XPATH, "//*[@id='username']")
        username_element.send_keys(username)

    def enter_snowflake_password(self, password):
        password_element = self.driver.find_element(By.XPATH, "//*[@id='password']")
        password_element.send_keys(password)

    def enter_snowflake_database(self, database):
        database_element = self.driver.find_element(By.CSS_SELECTOR, "[name='database']")
        database_element.send_keys(database)

    def enter_snowflake_warehouse(self, warehouse):
        warehouse_element = self.driver.find_element(By.CSS_SELECTOR, "[name='warehouse']")
        warehouse_element.clear()
        warehouse_element.send_keys(warehouse)

    def create_snowflake_source_endpoint(self, endpoint_name):
        """Creates a Snowflake source endpoint using the parameters in the config.ini file
        :param endpoint_name: The name of the endpoint. """
        self.new_endpoint_connection()
        self.choose_snowflake_type()
        self.choose_snowflake_user_pass_auth()
        self.enter_snowflake_server(self.config.get_section('Snowflake_DB')['server'])
        self.enter_snowflake_username(self.config.get_section('Snowflake_DB')['user'])
        self.enter_snowflake_password(self.config.get_section('Snowflake_DB')['password'])
        self.enter_snowflake_database(self.config.get_section('Snowflake_DB')['database'])
        self.enter_snowflake_warehouse(self.config.get_section('Snowflake_DB')['warehouse'])
        self.enter_endpoint_description('Snowflake Source Endpoint')
        self.enter_endpoint_name(endpoint_name)
        self.test_connection_valid()
        self.save()
        print("Created a Snowflake source endpoint:", endpoint_name)



    """------------------ MongoDB endpoint: Specific Methods ------------------"""

    def choose_mongodb_type(self):
        self.choose_endpoint_type("MongoDB")
        mongodb_type = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//li//*[text()='MongoDB']")))
        safe_click(mongodb_type)
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='host']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Save']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Test Connection']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Advanced']")))

    def enter_mongodb_host(self, name: str):
        server = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='host']")))
        server.send_keys(name)

    def enter_mongodb_username(self, username):
        username_element = self.driver.find_element(By.XPATH, "//*[@id='username']")
        username_element.send_keys(username)

    def enter_mongodb_password(self, password):
        password_element = self.driver.find_element(By.XPATH, "//*[@id='password']")
        password_element.send_keys(password)

    def enter_mongodb_database(self, database):
        database_element = self.driver.find_element(By.XPATH, "//*[@id='dbName']")
        database_element.clear()
        database_element.send_keys(database)

    def create_mongodb_source_endpoint(self, endpoint_name):
        """Creates a MongoDB source endpoint using the parameters in the config.ini file
        :param endpoint_name: The name of the endpoint. """
        self.new_endpoint_connection()
        self.choose_mongodb_type()
        self.enter_mongodb_host(f"{self.config.get_section('MongoDB_DB')['host']}:{self.config.get_section('MongoDB_DB')['port']}")
        self.enter_mongodb_username(self.config.get_section('MongoDB_DB')['user'])
        self.enter_mongodb_password(self.config.get_section('MongoDB_DB')['password'])
        self.enter_mongodb_database(self.config.get_section('MongoDB_DB')['database'])
        self.enter_endpoint_description('MongoDB Source Endpoint')
        self.enter_endpoint_name(endpoint_name)
        self.test_connection_valid()
        self.save()
        print("Created a MongoDB source endpoint:", endpoint_name)



    """------------------ IMS endpoint: Specific Methods ------------------"""

    def choose_ims_type(self):
        self.choose_endpoint_type("IBM IMS")
        ims_type = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//li//*[text()='IBM IMS']")))
        safe_click(ims_type)
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='server']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Save']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Test Connection']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Advanced']")))

    def enter_ims_server(self, name: str):
        server = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='server']")))
        server.send_keys(name)

    def enter_ims_port(self, port):
        port_element = self.driver.find_element(By.CSS_SELECTOR, "[label='port']>div>div:nth-of-type(2)>div>input")
        port_element.clear()
        port_element.send_keys(port)

    def enter_ims_connect_server(self, server):
        connect_server = self.driver.find_element(By.XPATH, "//*[@id='imsConnectServer']")
        connect_server.send_keys(server)

    def enter_ims_connect_port(self, port):
        port_element = self.driver.find_element(By.CSS_SELECTOR, "[number-data='model.imsConnectPort']>input")
        port_element.clear()
        port_element.send_keys(port)

    def enter_ims_username(self, username):
        username_element = self.driver.find_element(By.XPATH, "//*[@id='user']")
        username_element.send_keys(username)

    def enter_ims_password(self, password):
        password_element = self.driver.find_element(By.XPATH, "//*[@id='password']")
        password_element.send_keys(password)

    def enter_ims_psb(self, psb):
        psb_element = self.driver.find_element(By.XPATH, "//*[@id='psb']")
        psb_element.send_keys(psb)

    def enter_ims_pcb(self, pcb):
        pcb_element = self.driver.find_element(By.XPATH, "//*[@id='pcb']")
        pcb_element.send_keys(pcb)

    def enter_ims_dbd_xml(self, xml_location):
        dbd_xml_element = self.driver.find_element(By.XPATH, "//*[@id='dbd']")
        dbd_xml_element.send_keys(xml_location)

    def create_ims_source_endpoint(self, endpoint_name):
        """Creates an IMS source endpoint using the parameters in the config.ini file"""
        self.new_endpoint_connection()
        self.choose_ims_type()
        self.enter_ims_server(self.config.get_section('IMS_DB')['server'])
        self.enter_ims_port(self.config.get_section('IMS_DB')['server_port'])
        self.enter_ims_connect_server(self.config.get_section('IMS_DB')['host'])
        self.enter_ims_connect_port(self.config.get_section('IMS_DB')['port'])
        self.enter_ims_username(self.config.get_section('IMS_DB')['user'])
        self.enter_ims_password(self.config.get_section('IMS_DB')['password'])
        self.enter_ims_psb(self.config.get_section('IMS_DB')['psb'][0:6])
        self.enter_ims_pcb(self.config.get_section('IMS_DB')['schema'])
        self.enter_ims_dbd_xml(self.config.get_section('IMS_DB')['dbd_xml'])
        self.enter_endpoint_description('IMS Source Endpoint')
        self.enter_endpoint_name(endpoint_name)
        self.test_connection_valid()
        self.save()
        print("Created a IMS source endpoint:", endpoint_name)

    def create_custom_ims_source_endpoint(self, endpoint_name, description, dbd_xml, schema, psb, password, user, port, host, server_port, server):
        """Creates custom IMS source endpoint using parameters inserted by the user"""
        self.new_endpoint_connection()
        self.choose_ims_type()
        self.enter_ims_server(server)
        self.enter_ims_port(server_port)
        self.enter_ims_connect_server(host)
        self.enter_ims_connect_port(port)
        self.enter_ims_username(user)
        self.enter_ims_password(password)
        self.enter_ims_psb(psb)
        self.enter_ims_pcb(schema)
        self.enter_ims_dbd_xml(dbd_xml)
        self.enter_endpoint_description(description)
        self.enter_endpoint_name(endpoint_name)
        self.test_connection_valid()
        self.save()
        print("Created a IMS source endpoint:", endpoint_name)

    def test_connection(self):
        """Click the 'Test Connection' button to test the database connection."""
        test_connection_element = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Test Connection']")))
        safe_click(test_connection_element)

    def test_connection_valid(self):
        """Click the 'Test Connection' button to test the database connection and verifies the connection is valid."""
        self.test_connection()
        self.wait.until(EC.visibility_of_element_located((By.XPATH, "//span[contains(@custom-tooltip-text,'Test connection succeeded')]")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Save']")))

    def save(self):
        """Click the 'Save' button to save the endpoint configuration."""
        save_button = self.driver.find_element(By.XPATH, "//button[text()='Save']")
        safe_click(save_button)
        self.wait.until(EC.visibility_of_element_located((By.XPATH, "//*[text()='Database was successfully saved']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Delete']")))
        self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Duplicate']")))
        self.driver.find_element(By.XPATH, "//*[@id='endpointViewContainer']/div[1]/span[2]").click()

    def close(self):
        """Click the 'Close' button to close 'Manage Endpoints Connection' page."""
        close_button = self.driver.find_element(By.XPATH, "//*[text()='Close']")
        safe_click(close_button)

    def delete_endpoint(self, endpoint_name):
        """ Delete an endpoint entirely using a different method.
            :param endpoint_name: The name of the endpoint to be deleted. """
        endpoint = self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//*[text()='{endpoint_name}']")))
        endpoint.click()
        delete_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Delete']")))
        delete_button.click()
        ok_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='OK']")))
        ok_button.click()
        self.wait.until(EC.invisibility_of_element_located((By.XPATH, f"//*[text()='{endpoint_name}']")))
        print("Deleted endpoint:", endpoint_name)

