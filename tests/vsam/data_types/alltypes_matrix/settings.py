from tests.vsam.vsam_setup_env import *


def create_endpoints(vsam_test: SimpleNamespace):
    vsam_test.tasks_general_page.enter_manage_endpoints()
    vsam_test.vsam_source_name = vsam_test.manage_endpoints.random_endpoint_name('VSAM_DB')

    target_section = vsam_test.target_db.section_name
    vsam_test.target_name = vsam_test.manage_endpoints.random_endpoint_name(target_section)

    vsam_test.manage_endpoints.create_custom_vsam_source_endpoint(
        vsam_test.vsam_source_name, 'VSAM Source Endpoint', zip_metadata_file=vsam_test.zip_metadata_file)

    create_method_name = vsam_test.target_db.config["create_endpoint_method"]
    create_method = getattr(vsam_test.manage_endpoints, create_method_name)
    create_method(vsam_test.target_name)

    vsam_test.manage_endpoints.close()


def create_task(vsam_test: SimpleNamespace):
    create_endpoints(vsam_test)  # sets vsam_test.vsam_source_name / target_name
    task_name = vsam_test.test_dir_name
    vsam_test.tasks_general_page.create_new_task()
    new_task_name = vsam_test.new_task_page.new_task_creation(task_name)
    vsam_test.replicate_actions.task_data_loader()
    vsam_test.designer_page.choose_source_target(vsam_test.vsam_source_name, vsam_test.target_name)
    vsam_test.designer_page.enter_table_selection()
    vsam_test.table_selection.select_chosen_tables("ALLTYPES")
    vsam_test.designer_page.enter_task_settings()
    vsam_test.task_settings.set_task_settings_general()
    vsam_test.task_name = new_task_name
    return new_task_name