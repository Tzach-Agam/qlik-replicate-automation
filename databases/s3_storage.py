import boto3
import pandas as pd
from io import StringIO
import time
from configurations.config_manager import ConfigurationManager

class S3Storage:
    """ A class for interacting with Amazon S3 storage.
        This class provides methods for creating and managing directories in an S3 bucket, as well as combining files
        from a directory into a single CSV file with a distinction between files. """

    def __init__(self, config_manager: ConfigurationManager, section):
        """ Initialize the S3Storage instance.
            :param config_manager: An instance of ConfigurationManager for accessing S3 configuration.
            :param section: The name of the configuration section containing S3 access credentials and bucket name. """

        self.config = config_manager.get_section(section)
        self.access_key = self.config['access_key']
        self.secret_key = self.config['secret_key']
        self.bucket_name = self.config['bucket_name']
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

    def create_directory(self, directory_name):
        """ Create a directory in the S3 bucket.
            :param directory_name: The name of the directory to create. """

        try:
            directory_name = directory_name.strip('/')
            self.s3.put_object(Bucket=self.bucket_name, Key=f'{directory_name}/')
            print(f'Directory "{directory_name}" created in bucket "{self.bucket_name}"')
        except Exception as e:
            print(f'Error creating directory: {str(e)}')

    def delete_directory(self, directory_name):
        """ Delete a directory and its contents from the S3 bucket.
            :param directory_name: The name of the directory to delete. """

        try:
            directory_name = directory_name.strip('/')
            objects = self.s3.list_objects(Bucket=self.bucket_name, Prefix=directory_name)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    self.s3.delete_object(Bucket=self.bucket_name, Key=obj['Key'])
                print(f'Directory "{directory_name}" and its contents deleted from bucket "{self.bucket_name}"')
            else:
                print(f'Directory "{directory_name}" not found in bucket "{self.bucket_name}"')
        except Exception as e:
            print(f'Error deleting directory: {str(e)}')

    def wait_for_files_in_directory(self, directory_name, desired_file_count, seconds_to_wait=10):
        """ Wait until the specified number of files are present in the directory in the S3 bucket.
            :param directory_name: The name of the directory (prefix) to monitor.
            :param desired_file_count: The number of files to wait for.
            :param seconds_to_wait: The interval (in seconds) between checks for file count. """

        try:
            directory_name = directory_name.strip('/')
            while True:
                objects = self.s3.list_objects(Bucket=self.bucket_name, Prefix=directory_name)
                if 'Contents' in objects and len(objects['Contents']) >= desired_file_count:
                    print(f'Found {len(objects["Contents"])} files in directory "{directory_name}"')
                    break
                else:
                    print(f'Waiting for {desired_file_count} files in directory "{directory_name}"...')
                    time.sleep(seconds_to_wait)
        except Exception as e:
            print(f'Error waiting for files in directory: {str(e)}')

    def combine_files_to_csv_distinct(self, directory_name, output_csv_file):
        """ Combine files from a directory in the S3 bucket into one CSV file with a distinction between files.
            :param directory_name: The name of the directory (prefix) containing files to combine.
            :param output_csv_file: The name of the output CSV file. """

        try:
            directory_name = directory_name.strip('/')
            objects = self.s3.list_objects(Bucket=self.bucket_name, Prefix=directory_name)

            if 'Contents' in objects:
                file_contents = []
                for i, obj in enumerate(objects['Contents']):
                    file_obj = self.s3.get_object(Bucket=self.bucket_name, Key=obj['Key'])
                    file_content = file_obj['Body'].read().decode('utf-8')

                    # Add a distinction to each file's content (e.g., a header with the file name)
                    file_content_with_distinction = f'Directory {directory_name} - File {i},\n{file_content}\n'

                    file_contents.append(file_content_with_distinction)

                combined_csv = '\n'.join(file_contents)
                with StringIO(combined_csv) as combined_csv_file:
                    df = pd.read_csv(combined_csv_file, delimiter=':', header=None)
                    df.to_csv(output_csv_file, index=False, header=False)
                print(f'Combined files from "{directory_name}" with distinction and saved as "{output_csv_file}"')
            else:
                print(f'Directory "{directory_name}" not found in bucket "{self.bucket_name}"')
        except Exception as e:
            print(f'Error combining files to CSV: {str(e)}')
