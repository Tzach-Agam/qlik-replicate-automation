import configparser

class ConfigurationManager:
    """Utility for managing configuration settings using a configuration file (config.ini)."""

    def __init__(self, config_file: str):
        """Initialize the ConfigurationManager class object
        :param config_file: Path to the configuration file (config.ini)."""

        self.config_file = config_file
        self.config = self.read_config()

    def read_config(self):
        """Reads and parses the configuration file."""

        config = configparser.ConfigParser()
        config.read(self.config_file)
        return config

    def get_section(self, section: str):
        """ Gets a specific section from the configuration file (config file). Returns a dictionary representing the
            section's settings.
            :param section: The name of the section in the configuration file"""

        return self.config[section]

    def get_base_url(self):
        """Returns the base URL setting from the 'Website' section in config.ini. """

        return self.config.get('Website', 'base_url')

    def get_default_schemas(self):
        """Gets default schema settings from the 'Default_Schemas' in config.ini.
        Returns the names of the source schema, target schema, and control schema."""

        source_schema = self.config.get('Default_Schemas', 'source_schema')
        target_schema = self.config.get('Default_Schemas', 'target_schema')
        control_schema = self.config.get('Default_Schemas', 'control_schema')
        return source_schema, target_schema, control_schema

    def source_tasklog_path(self):
        """Gets and returns the path to the directory with all the task logs of replicate software
        (source path of task logs)."""

        source_log_path = self.config.get('Task_log_Path', 'source_directory')
        return source_log_path

    def sql_logs_results_path(self):
        """Gets and returns paths to directory's of task logs and good files for the sql server database tests."""

        task_logs_path = self.config.get('Task_log_Path', 'sql_task_logs_dir')
        good_files_path = self.config.get('Good_Files_Path', 'sql_good_files')
        return task_logs_path, good_files_path

    def oracle_logs_results_path(self):
        """Gets and returns paths to directory's of task logs and good files for oracle database tests."""

        task_logs_path = self.config.get('Task_log_Path', 'oracle_task_logs_dir')
        good_files_path = self.config.get('Good_Files_Path', 'oracle_good_files')
        return task_logs_path, good_files_path

    def mysql_logs_results_path(self):
        """Gets and returns paths to directory's of task logs and good files for mysql database tests."""

        task_logs_path = self.config.get('Task_log_Path', 'mysql_task_logs_dir')
        good_files_path = self.config.get('Good_Files_Path', 'mysql_good_files')
        return task_logs_path, good_files_path

    def postgres_logs_results_path(self):
        """Gets and returns paths to directory's of task logs and good files for postgres database tests."""

        task_logs_path = self.config.get('Task_log_Path', 'postgres_task_logs_dir')
        good_files_path = self.config.get('Good_Files_Path', 'postgres_good_files')
        return task_logs_path, good_files_path

    def s3_logs_results_path(self):
        """Gets and returns paths to directory's of task logs and good files for postgres database tests."""

        task_logs_path = self.config.get('Task_log_Path', 's3_tasks_logs_dir')
        good_files_path = self.config.get('Good_Files_Path', 's3_good_files')
        return task_logs_path, good_files_path


