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

    def get_driver(self):
        """Gets a specific driver from the configuration file (config file)."""
        return self.config.get("Browser", "driver")

    def get_headless(self):
        """Returns the value of the headless option, whether true or false """
        return self.config.getboolean('Display_Mode', 'headless')

    def get_secure_login(self):
        """Returns whether replicate will log in with secure method """
        return self.config.getboolean('Login_Method', 'secure_login')

    def get_delete_task_endpoint(self):
        """Returns whether replicate will delete task endpoint at the end of the task"""
        delete_task = self.config.getboolean("Clean_Replicate", "task")
        delete_endpoint = self.config.getboolean("Clean_Replicate", "endpoint")
        return delete_task, delete_endpoint

    def get_base_url(self):
        """Returns the base URL setting from the 'Website' section in config.ini. """
        return self.config.get('Website', 'base_url')

    def get_username(self):
        """Returns the username setting from the 'Website' section in config.ini. """
        return self.config.get('Credentials', 'username')

    def get_password(self):
        """Returns the password setting from the 'Website' section in config.ini. """
        return self.config.get('Credentials', 'password')

    def get_login_url(self):
        """Returns the login URL setting from the 'Website' section in config.ini. """
        base_url = self.get_base_url()
        username = self.get_username()
        password = self.get_password()
        login_url = base_url[0:8] + username + ':' + password + '@' + base_url[8:] + '/login/'
        return login_url

    def get_default_schemas(self):
        """Gets default schema settings from the 'Default_Schemas' in config.ini.
        Returns the names of the source schema, target schema, and control schema."""
        source_schema = self.config.get('Default_Schemas', 'source_schema')
        target_schema = self.config.get('Default_Schemas', 'target_schema')
        control_schema = self.config.get('Default_Schemas', 'control_schema')
        return source_schema, target_schema, control_schema

    def get_default_tables(self):
        """Gets the default table name from the 'Default_Table' section in config.ini."""
        default_table_name = self.config.get('Default_Tables', 'default_table_name')
        sync_table_name = self.config.get('Default_Tables', 'sync_table_name')
        return default_table_name, sync_table_name

    def source_tasklog_path(self):
        """Gets and returns the path to the directory with all the task task_logs of replicate software
        (source path of task task_logs)."""
        source_log_path = self.config.get('Task_log_Path', 'source_directory')
        return source_log_path

    def get_enabled_targets(self):
        """Returns a list of targets that are enabled (True) in the [Targets] section."""
        if 'Targets' not in self.config:
            return []  # no targets section, return empty list

        enabled_targets = [
            target for target, value in self.config['Targets'].items()
            if value.lower() == 'true'
        ]
        return enabled_targets