from selenium.common.exceptions import *


def safe_click(element):
    """ Safely clicks a web element while handling common exceptions.
        :param element: The web element to click."""

    try:
        element.click()
    except (NoSuchElementException, ElementClickInterceptedException,
            ElementNotInteractableException, ElementNotSelectableException,) as e:
        print(f"Error clicking element: {e}")