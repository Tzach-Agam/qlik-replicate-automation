"""A module for creating Selenium WebDriver instances for various browsers and modes.

This module provides functions to create WebDriver instances for Chrome, Edge, and Firefox browsers, as well as headless
modes for these browsers.

Example:
    To create a Chrome WebDriver instance:
    driver = chrom_driver()

    To create a headless Firefox WebDriver instance:
    headless_driver = headless_firefox_driver()"""

from selenium import webdriver


"""----------------------------------- Running with the browser open -----------------------------------"""


def chrom_driver():
    from selenium.webdriver.chrome.service import Service
    serv_obj = Service("C:\\Users\\JUJ\PycharmProjects\\qlik_replicate_automation\\browsers\chromedriver.exe")
    driver = webdriver.Chrome(service=serv_obj)
    return driver


def edge_driver():
    from selenium.webdriver.edge.service import Service
    serv_obj = Service("C:\\Users\\JUJ\PycharmProjects\\qlik_replicate_automation\\browsers\msedgedriver.exe")
    driver = webdriver.Edge(service=serv_obj)
    return driver


def firefox_driver():
    from selenium.webdriver.firefox.service import Service
    serv_obj = Service("C:\\Users\\JUJ\PycharmProjects\\qlik_replicate_automation\\browsers\geckodriver.exe")
    driver = webdriver.Firefox(service=serv_obj)
    return driver


"""----------------------------------- Running in the background -----------------------------------"""


def headless_chrom_driver():
    from selenium.webdriver.chrome.service import Service
    serv_obj = Service("C:\\Users\\JUJ\PycharmProjects\\qlik_replicate_automation\\browsers\chromedriver.exe")
    ops = webdriver.ChromeOptions()
    ops.headless = True
    driver = webdriver.Chrome(service=serv_obj, options=ops)
    return driver


def headless_edge_driver():
    from selenium.webdriver.edge.service import Service
    serv_obj = Service("C:\\Users\\JUJ\PycharmProjects\\qlik_replicate_automation\\browsers\msedgedriver.exe")
    ops = webdriver.EdgeOptions()
    ops.headless = True
    driver = webdriver.Chrome(service=serv_obj, options=ops)
    return driver


def headless_firefox_driver():
    from selenium.webdriver.firefox.service import Service
    serv_obj = Service("C:\\Users\\JUJ\PycharmProjects\\qlik_replicate_automation\\browsers\geckodriver.exe")
    ops = webdriver.FirefoxOptions()
    ops.headless = True
    driver = webdriver.Chrome(service=serv_obj, options=ops)
    return driver
