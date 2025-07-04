"""A module for creating Selenium WebDriver instances for various browsers and modes.

This module provides functions to create WebDriver instances for Chrome, Edge, and Firefox browsers.
The function uses incognito mode, accepts insecure certificates, runs in headless mode, and disables automation detection.

Example:
    To create a Chrome WebDriver instance:
    driver = chrom_driver()"""

from selenium import webdriver

def chrom_driver():
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.chrome.options import Options
    service = ChromeService(ChromeDriverManager().install())
    options = Options()
    options.add_argument("--incognito")
    options.accept_insecure_certs = True
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    #options.add_argument('--headless=new')
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def edge_driver():
    from webdriver_manager.microsoft import EdgeChromiumDriverManager
    from selenium.webdriver.edge.service import Service as EdgeService
    from selenium.webdriver.edge.options import Options
    service = EdgeService(EdgeChromiumDriverManager().install())
    options = Options()
    options.add_argument("--incognito")
    options.accept_insecure_certs = True
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    #options.add_argument('--headless=new')
    driver = webdriver.Edge(service=service, options=options)
    return driver

def firefox_driver():
    from webdriver_manager.firefox import GeckoDriverManager
    from selenium.webdriver.firefox.service import Service as FirefoxService
    from selenium.webdriver.firefox.options import Options
    service = FirefoxService(GeckoDriverManager().install())
    options = Options()
    options.add_argument("--incognito")
    options.accept_insecure_certs = True
    #options.add_argument('--headless=new')
    driver = webdriver.Firefox(service=service, options=options)
    return driver
