from types import SimpleNamespace
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from configurations.config_manager import ConfigurationManager
from browsers.browsers import get_webdriver
from replicate_pages import *
import pytest
from pathlib import Path

@pytest.fixture(scope="session")
def replicate_env():
    config = ConfigurationManager(
        str(Path(__file__).resolve().parents[3] / "configurations" / "config.ini"))
    driver = get_webdriver(config)
    wait = WebDriverWait(driver, 120)
    replicate_actions = ReplicateCommonActions(driver, config)
    tasks_general_page = TasksPage(driver)
    manage_endpoints = ManageEndpoints(driver, config)
    replicate_actions.open_replicate_software()
    replicate_actions.set_windows_size()
    driver.implicitly_wait(3)
    replicate_actions.loader_icon_opening_replicate()
    env = SimpleNamespace(config=config, driver=driver, wait=wait, tasks_general_page=tasks_general_page,
                          manage_endpoints=manage_endpoints, replicate_actions=replicate_actions)

    yield env

    driver.quit()

@pytest.fixture(scope="function")
def ims_endpoint(replicate_env):
    replicate_env.tasks_general_page.enter_manage_endpoints()
    endpoint_name = replicate_env.manage_endpoints.random_endpoint_name('IMS_DB')

    yield endpoint_name

    replicate_env.manage_endpoints.save()
    replicate_env.manage_endpoints.delete_endpoint(endpoint_name)
    replicate_env.manage_endpoints.close()

def test_connection_valid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint')
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "Test connection succeeded" in message_text, f"Expected success message not displayed. Got: {message_text}"
    print(f"Test passed: Connection PASSED as expected with message - {message_text}")


'''Host tests'''
def test_host_wrong_zos(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', host="zos6")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, UNAVAILABLE: io exception" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_host_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', host="Kofiko123")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, UNAVAILABLE: Unable to resolve host Kofiko123." in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_host_empty(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', host=" ")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, java.lang.IllegalArgumentException: Invalid host or port: 50052" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")


'''Port tests'''
def test_port_zero(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', port="0")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, UNAVAILABLE: io exception" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_port_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', port="22222")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, UNAVAILABLE: io exception" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")


'''IMS Connect Host tests'''
#Note: Error message very long - may need to be changed in future
def test_ims_connet_server_different_zos(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', ims_connect_host="zos6.qliktech.com")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_ims_connet_server_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', ims_connect_host="Kofiko123")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

#Potential bug - empty ims connect server results in connection succeed
def test_ims_connet_server_empty(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', ims_connect_host=" ")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")


''' IMS Connect port tests'''
def test_ims_connect_port_zero(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', ims_connect_port="0")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_ims_connect_port_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', ims_connect_port="22222")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")


''' Username tests'''
def test_username_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', user="Kofiko123")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

#Potential bug - empty username results in connection succeed
def test_username_empty(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', user=" ")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")


''' Password tests'''
#Potential bug - invalid password results in connection succeed
def test_password_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', password="Kofiko123")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

#Potential bug - empty password results in connection succeed
def test_password_empty(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', password=" ")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")


''' PSB tests'''
def test_psb_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', psb="Kofiko12")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, INTERNAL: Unable to retrieve metadata information for Database (PSB)" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_psb_empty(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', psb=" ")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, INTERNAL: Unable to retrieve metadata information for Database (PSB)" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")


''' PCB tests'''
def test_pcb_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', pcb="Kofiko12")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, INTERNAL: The PCB name KOFIKO12 was not found in the PSB DEVPSB for database url DEVPSB" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_pcb_empty(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', pcb=" ")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, INTERNAL: The PCB name was not found in the PSB DEVPSB for database url DEVPSB" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")


''' DBD XML tests'''
def test_dbd_xml_path_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', dbd_xml="C:\invalid_path\IMSDEV_ZOS9\DBD\IMSDEV.dbd")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, java.io.FileNotFoundException: C:\invalid_path\IMSDEV_ZOS9\DBD\IMSDEV.dbd (The system cannot find the path specified)" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_dbd_xml_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', dbd_xml="C:\\Users\JUJ\PycharmProjects\qlik-replicate-automation\\tests\ims\dbd_files\INVALID_DBD.dbd")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, com.ibm.ims.dli.DLIException: Unable to create document from XML" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_dbd_xml_empty(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', dbd_xml=" ")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, java.io.FileNotFoundException" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")


''' LogStream tests'''
def test_logstream_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', logstream_name="Kofiko123")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_logstream_empty(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', logstream_name=" ")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "Test connection succeeded" in message_text, f"Expected success message not displayed. Got: {message_text}"
    print(f"Test passed: Connection PASSED as expected with message - {message_text}")


''' Security tests'''
def test_client_certificate_path_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', client_certificate="C:\IMS\cert\client\Kofiko123.p12")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, java.io.FileNotFoundException: C:\IMS\cert\client\Kofiko123.p12 (The system cannot find the file specified)" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_client_certificate_content_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', client_certificate="C:\IMS\cert\cert.p12")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, java.io.IOException: keystore password was incorrect" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_client_certificate_empty(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', client_certificate=" ")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, java.io.FileNotFoundException" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")


def test_client_password_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', client_password="Kofiko123")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, java.io.IOException: keystore password was incorrect" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_client_password_empty(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', client_password=" ")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, java.io.IOException: keystore password was incorrect" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")


def test_trusted_ca_path_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', trusted_ca="C:\invalid\path\IMS\cert\cert.pem")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, java.io.FileNotFoundException: C:\invalid\path\IMS\cert\cert.pem (The system cannot find the path specified)" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_trusted_ca_content_invalid(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', trusted_ca="C:\IMS\cert\client\cert.pem")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, UNAVAILABLE: io exception Channel Pipeline" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")

def test_trusted_ca_empty(replicate_env, ims_endpoint):
    replicate_env.manage_endpoints.create_custom_ims_source_endpoint2(ims_endpoint, 'IMS Source Endpoint', trusted_ca=" ")
    replicate_env.manage_endpoints.test_connection()
    connection_message = replicate_env.wait.until(EC.visibility_of_element_located(
            (By.XPATH, "//span[@class='ng-binding ng-scope']")))
    message_text = connection_message.text
    assert "SYS-E-HTTPFAIL, java.io.FileNotFoundException" in message_text, f"Expected failure message not displayed. Got: {message_text}"
    print(f"Test passed: Connection FAILED as expected with message - {message_text}")