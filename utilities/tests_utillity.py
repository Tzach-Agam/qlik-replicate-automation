from time import sleep

class TestUtilities:
    """Utility class for automating task and endpoint creation in a testing environment."""

    @staticmethod
    def task_creation(tasks_general_page, new_task_page, designer_page, table_selection, task_settings, task_name,
                      source_data, target_data, source_schema, target_schema, control_schema):
        """Automates the creation of task on replicate software according to the given values."""

        tasks_general_page.create_new_task()
        new_task_page.new_task_creation(f'{task_name}', 'task')
        designer_page.choose_source_target(source_data, target_data)
        designer_page.enter_table_selection()
        table_selection.choose_source_schema(source_schema)
        designer_page.enter_task_settings()
        task_settings.set_task_settings_general(target_schema, control_schema)

    @staticmethod
    def endpoints_creation(tasks_general_page, manage_endpoints, source_endpoint, target_endpoint):
        """Automates the creation of source and target endpoints on replcatr software."""

        tasks_general_page.enter_manage_endpoints()
        manage_endpoints.create_rdbms_endpoint2(source_endpoint)
        sleep(5)
        manage_endpoints.close()
        sleep(5)
        tasks_general_page.enter_manage_endpoints()
        manage_endpoints.create_rdbms_endpoint2(target_endpoint)
        sleep(5)
        manage_endpoints.close()