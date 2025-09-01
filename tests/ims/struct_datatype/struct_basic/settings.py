from tests.ims.ims_setup_env import *

def create_endpoints(ims_test: SimpleNamespace):
    ims_test.tasks_general_page.enter_manage_endpoints()
    ims_test.ims_source_name = ims_test.manage_endpoints.random_endpoint_name('IMS_DB')

    target_section = ims_test.target_db.section_name
    ims_test.target_name = ims_test.manage_endpoints.random_endpoint_name(target_section)

    ims_test.manage_endpoints.create_custom_ims_source_endpoint(
        ims_test.ims_source_name, 'IMS Source Endpoint', "ATTUNITY.IMS.DCAPDATA",
        ims_test.dbd_file,
        "DEVPCB", "DEVPSB", "VICTORK", "VICTORK", 5555, "zos9.qliktech.com", 50052, "zos9.qliktech.com"
    )

    create_method_name = ims_test.target_db.config["create_endpoint_method"]
    create_method = getattr(ims_test.manage_endpoints, create_method_name)
    create_method(ims_test.target_name)

    ims_test.manage_endpoints.close()

def create_task(ims_test: SimpleNamespace):
    create_endpoints(ims_test)  # sets ims_test.target_name
    task_name = f"IMS2{ims_test.target_db.config['endpoint']}STCTBASE"
    ims_test.tasks_general_page.create_new_task()
    new_task_name = ims_test.new_task_page.new_task_creation(task_name)
    ims_test.replicate_actions.task_data_loader()
    ims_test.designer_page.choose_source_target(ims_test.ims_source_name, ims_test.target_name)
    ims_test.designer_page.enter_table_selection()
    ims_test.table_selection.select_chosen_tables("STRUCT4")
    ims_test.designer_page.enter_task_settings()
    ims_test.task_settings.set_task_settings_general()
    ims_test.task_name = new_task_name
    return new_task_name

