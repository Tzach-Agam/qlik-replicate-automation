import os
import shutil
import filecmp
import random
import time

from configurations.config_manager import ConfigurationManager
from replicate_pages import ReplicateCommonActions
from selenium.common.exceptions import *

"""
Utility Functions for Qlik Replicate Automation project.
This module contains a set of utility functions to simplify common tasks in the project. 
These functions are designed to provide error handling and make it easier to interact with web elements files and task_logs.
"""


def safe_click(element, max_retries=3):
    """Safely clicks a web element while handling common exceptions.
    :param element: The web element to click.
    :param max_retries: Number of retries for stale elements.
    """
    from selenium.common.exceptions import StaleElementReferenceException
    import time

    for attempt in range(max_retries):
        try:
            element.click()
            return
        except StaleElementReferenceException:
            if attempt == max_retries - 1:
                raise
            time.sleep(0.3)
        except (NoSuchElementException, ElementClickInterceptedException,
                ElementNotInteractableException, ElementNotSelectableException) as e:
            print(f"Error clicking element: {e}")
            raise


def move_file_to_target_dir(source_dir: str, target_dir: str, file_name: str,
                            config: ConfigurationManager, rep_common_actions: ReplicateCommonActions, task_name=None,
                            retries: int = 10, delay: float = 1.0):
    """Copies a chosen file from source dir to target dir with retries if the file is still locked.

    :param source_dir: path of the source directory
    :param target_dir: path of the target directory
    :param file_name: name of the file to be copied
    :param config: ConfigurationManager for base_url
    :param rep_common_actions: ReplicateCommonActions for downloading logs
    :param task_name: task name (used when downloading logs remotely)
    :param retries: number of retries while waiting for file
    :param delay: delay (seconds) between retries
    """

    hostname = os.environ.get('COMPUTERNAME')
    source_path = os.path.join(source_dir, file_name)
    target_path = os.path.join(target_dir, file_name)

    if hostname is None:
        print("Hostname could not be determined. Skipping file transfer.")
        return

    def wait_for_file_ready(path: str) -> bool:
        """Wait until file exists and is readable."""
        for _ in range(retries):
            if os.path.exists(path):
                try:
                    with open(path, "rb"):
                        return True
                except PermissionError:
                    pass
            time.sleep(delay)
        return False

    def handle_existing_target(path: str):
        """Rename existing target file to avoid overwriting."""
        base_name, extension = os.path.splitext(file_name)
        while True:
            random_suffix = str(random.randint(1, 99999))
            renamed_file = f"{base_name}_{random_suffix}{extension}"
            renamed_path = os.path.join(target_dir, renamed_file)
            if not os.path.exists(renamed_path):
                os.rename(path, renamed_path)
                print(f"Renamed existing target file to: {renamed_path}")
                break

    # Remote machine branch
    if hostname.lower() not in config.get_base_url().lower():
        print(f"Test runs on a Replicate machine that is NOT project host machine: {hostname}.")
        source_path = os.path.join(config.downloaded_files_path(), file_name)
        if os.path.exists(source_path):
            print(f"Deleting existing file in source dir: {source_path}")
            os.remove(source_path)
        rep_common_actions.download_task_log(task_name)

        if os.path.exists(target_path):
            handle_existing_target(target_path)

        if not wait_for_file_ready(source_path):
            raise FileNotFoundError(f"File not ready after waiting: {source_path}")

        shutil.copy(source_path, target_path)
        print(f"Copied '{file_name}' to '{target_path}'")

    # Local machine branch
    else:
        print(f"Test runs on the same Replicate machine that is on project host machine: {hostname}.")
        if os.path.exists(target_path):
            handle_existing_target(target_path)

        if not wait_for_file_ready(source_path):
            raise FileNotFoundError(f"File not ready after waiting: {source_path}")

        shutil.copy(source_path, target_path)
        print(f"Copied '{file_name}' to '{target_path}'")


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
