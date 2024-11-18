import os
import shutil
import filecmp
import random
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

def move_file_to_target_dir(source_dir: str, target_dir: str, file_name: str):
    """ Moves a chosen file from source dir to target dir while preserving the file in target dir if it already exits.
        :param source_dir: the path of the source directory
        :param target_dir: the path of the target directory
        :param file_name: the name of the file that will be moved """

    source_path = os.path.join(source_dir, file_name)
    target_path = os.path.join(target_dir, file_name)

    if os.path.exists(target_path):
        base_name, extension = os.path.splitext(file_name)
        random_suffix = str(random.randint(1, 99999))
        new_file_name = f"{base_name}_{random_suffix}{extension}"
        target_path = os.path.join(target_dir, new_file_name)

    shutil.copy(source_path, target_path)
    print(f"Moved '{file_name}' to '{target_dir}'")

def compare_files(good_file, data_file):
    """ Compares files and deletes the data file if it's identical to the good file.
        :param good_file: The path for the good file, which is the expected result of test executed. The good file  contains
        the data that is expected to be in the target endpoint's tables after the end of the test and replication process.
        :param data_file: The path for the data file, which is the current result of the test. The data file contains the
        data in the target endpoint's tables after the end of the test and replication process.

     If the data in the good and data files is identical, it means that all the data in all the tables was replicated
     from the source to the target successfully, which means successful result of the test."""

    if filecmp.cmp(good_file, data_file, shallow=False):
        os.remove(data_file)
        print(f"Data file is deleted since it's identical to the Good file")
    else:
        raise AssertionError(f"Data file is not identical to Good file")

def log_finder_display(file_path, search_text):
    """ Searches for a specific text in a log file and return the lines containing the text.
        :param file_path: The path to the log file.
        :param search_text: The text to search for in the log file."""
    try:
        with open(file_path, 'r') as file:
            found_lines = []
            line_number = 1

            for line in file:
                if search_text in line:
                    found_lines.append((line_number, line))
                line_number += 1

            if found_lines:
                for line_number, line in found_lines:
                    print(f"Found '{search_text}' at line {line_number}: {line}")
                else:
                    print(f"'{search_text}' not found in the log file.")
            else:
                print(f"'{search_text}' not found in the log file.")

    except FileNotFoundError:
        return "File not found"
    except Exception as e:
        return f"An error occurred: {str(e)}"

def log_finder(file_path, search_text, result_file=None):
    """ Search for a specific text in a log file and count its occurrences.
        :param file_path: The path to the log file.
        :param search_text: The text to search for in the log file.
        :param result_file: The path to an output file to write the result. Default is None."""
    try:
        with open(file_path, 'r') as file:
            count = 0

            for line in file:
                if search_text in line:
                    count += 1

            if count == 0 and result_file:
                not_found_message = f"'{search_text}' not found in the log file."
                with open(result_file, 'a') as output:
                    output.write(not_found_message + "\n")
                print(not_found_message)

            elif count > 0 and result_file:
                result_message = f"'{search_text}' found {count} times in the log file."
                with open(result_file, 'a') as output:
                    output.write(result_message + "\n")
                print(result_message)

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        if result_file:
            with open(result_file, 'a') as output:
                output.write(error_message + "\n")
        print(error_message)
