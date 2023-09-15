from selenium.common.exceptions import *
"""
Utility Functions for Qlik Replicate Automation project.
This module contains a set of utility functions to simplify common tasks in the project. 
These functions are designed to provide error handling and make it easier to interact with web elements files and logs.
"""

def safe_click(element):
    """ Safely clicks a web element while handling common exceptions.
        :param element: The web element to click.
        :raises NoSuchElementException: If the element is not found on the web page.
        :raises ElementClickInterceptedException: If the click is intercepted by another element."""

    try:
        element.click()
    except (NoSuchElementException, ElementClickInterceptedException,
            ElementNotInteractableException, ElementNotSelectableException) as e:
        print(f"Error clicking element: {e}")
