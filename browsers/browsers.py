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
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service as ChromeService
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
    return driver


def edge_driver():
    from webdriver_manager.microsoft import EdgeChromiumDriverManager
    from selenium.webdriver.edge.service import Service as EdgeService
    driver = webdriver.Edge(service=EdgeService(EdgeChromiumDriverManager().install()))
    return driver


def firefox_driver():
    from webdriver_manager.firefox import GeckoDriverManager
    from selenium.webdriver.firefox.service import Service as FirefoxService
    driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))
    return driver


"""----------------------------------- Running in the background -----------------------------------"""


def headless_chrom_driver():
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service as ChromeService
    ops = webdriver.ChromeOptions()
    ops.headless = True
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=ops)
    return driver


def headless_edge_driver():
    from webdriver_manager.microsoft import EdgeChromiumDriverManager
    from selenium.webdriver.edge.service import Service as EdgeService
    ops = webdriver.EdgeOptions()
    ops.headless = True
    driver = webdriver.Chrome(service=EdgeService(EdgeChromiumDriverManager().install()), options=ops)
    return driver


def headless_firefox_driver():
    from webdriver_manager.firefox import GeckoDriverManager
    from selenium.webdriver.firefox.service import Service as FirefoxService
    ops = webdriver.FirefoxOptions()
    ops.headless = True
    driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()), options=ops)
    return driver
