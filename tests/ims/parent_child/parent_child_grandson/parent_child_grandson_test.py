from settings import *

def test_parent_child_grandson(ims_test):
    """Tests the relationship a parent-child-grandson in IMS source with FL and CDC"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC1'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC2'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC3'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC4'")
    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"PARENT\"")
    ims_test.ims_db.cursor.execute("DELETE FROM \"DVPCB\".\"CHILD\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ROOT\"  (ROOTID, FILL_0) VALUES ('ROOT000PC1', 'Parent_Child1')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ROOT\"  (ROOTID, FILL_0) VALUES ('ROOT000PC2', 'Parent_Child2')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ROOT\"  (ROOTID, FILL_0) VALUES ('ROOT000PC3', 'Parent_Child3')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"ROOT\"  (ROOTID, FILL_0) VALUES ('ROOT000PC4', 'Parent_Child4')")
    for i in range(1, 5):
        ims_test.ims_db.cursor.execute(
            f"INSERT INTO \"DVPCB\".\"PARENT\" (ROOT_ROOTID, PKEY, FIRST_NAME, MIDDLE_NAME, LAST_NAME)"
            f"VALUES('ROOT000PC1', 'PKEY{i}', 'Ben', 'Zion', 'Agam')")
        ims_test.ims_db.connection.commit()
    for i in range(1, 5):
        ims_test.ims_db.cursor.execute(
            f"INSERT INTO \"DVPCB\".\"PARENT\" (ROOT_ROOTID, PKEY, FIRST_NAME, MIDDLE_NAME, LAST_NAME)"
            f"VALUES('ROOT000PC2', 'PKEY{i}', 'Tzach', 'Itzh', 'AGAM')")
        ims_test.ims_db.connection.commit()
    for i in range(1, 5):
        ims_test.ims_db.cursor.execute(
            f"INSERT INTO \"DVPCB\".\"PARENT\" (ROOT_ROOTID, PKEY, FIRST_NAME, MIDDLE_NAME, LAST_NAME)"
            f"VALUES('ROOT000PC3', 'PKEY{i}', 'Dor', 'David', 'Agam')")
        ims_test.ims_db.connection.commit()
    for i in range(1, 5):
        ims_test.ims_db.cursor.execute(
            f"INSERT INTO \"DVPCB\".\"PARENT\" (ROOT_ROOTID, PKEY, FIRST_NAME, MIDDLE_NAME, LAST_NAME)"
            f"VALUES('ROOT000PC4', 'PKEY{i}', 'Shai', 'Shalo', 'Agam')")
        ims_test.ims_db.connection.commit()
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC1', 'PKEY1', 'CKEY1', 'Israel', 'Lod', 'Hubert Humphry 3', '3556', '21340')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC1', 'PKEY1', 'CKEY2', 'United States', 'Chicago', 'Wacker Dr', '77', '60601')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC1', 'PKEY1', 'CKEY3', 'Japan', 'Tokyo', 'Shibuya 1-2', '109', '1500042')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC1', 'PKEY1', 'CKEY4', 'United Kingdom', 'London', 'Baker Street', '221', '60601')")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC2', 'PKEY2', 'CKEY1', 'Israel', 'Lod', 'Hubert Humphry 3', '3556', '21340')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC2', 'PKEY2', 'CKEY2', 'United States', 'New York', 'Main St 1A', '1001', '10001')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC2', 'PKEY2', 'CKEY3', 'Germany', 'Berlin', 'Unter den Linden 7', '45', '10117')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC2', 'PKEY2', 'CKEY4', 'Canada', 'Toronto', 'Queen St W 15', '222', '2222')")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC3', 'PKEY3', 'CKEY1', 'Israel', 'Lod', 'Hubert Humphry 3', '3556', '21340')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC3', 'PKEY3', 'CKEY2', 'Spain', 'Madrid', 'Gran Vía', '45', '28013')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC3', 'PKEY3', 'CKEY3', 'Brazil', 'Rio de Janeiro', 'Avenida Atlântica', '1702', '22021001')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC3', 'PKEY3', 'CKEY4', 'India', 'Mumbai', 'Bandra Kurla Complex', '101', '400051')")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC4', 'PKEY4', 'CKEY1', 'Israel', 'Lod', 'Hubert Humphry 3', '3556', '21340')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC4', 'PKEY4', 'CKEY2', 'Ireland', 'Dublin', 'O’Connell Street', '83', '11223')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC4', 'PKEY4', 'CKEY3', 'Portugal', 'Lisbon', 'Rua Augusta', '250', '1100055')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC4', 'PKEY4', 'CKEY4', 'South Korea', 'Seoul', 'Gangnam-daero', '420', '06197')")

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('3')
    ims_test.monitor_page.cdc_tab()

    for i in range(11, 15):
        ims_test.ims_db.cursor.execute(
            f"INSERT  INTO \"DVPCB\".\"PARENT\" (ROOT_ROOTID, PKEY, FIRST_NAME, MIDDLE_NAME, LAST_NAME)"
            f"VALUES('ROOT000PC1', 'PKEY{i}', 'Ben', 'Zion', 'Agam')")
        ims_test.ims_db.connection.commit()
    for i in range(11, 15):
        ims_test.ims_db.cursor.execute(
            f"INSERT  INTO \"DVPCB\".\"PARENT\" (ROOT_ROOTID, PKEY, FIRST_NAME, MIDDLE_NAME, LAST_NAME)"
            f"VALUES('ROOT000PC2', 'PKEY{i}', 'Tzach', 'Itzh', 'AGAM')")
        ims_test.ims_db.connection.commit()
    for i in range(11, 15):
        ims_test.ims_db.cursor.execute(
            f"INSERT  INTO \"DVPCB\".\"PARENT\" (ROOT_ROOTID, PKEY, FIRST_NAME, MIDDLE_NAME, LAST_NAME)"
            f"VALUES('ROOT000PC3', 'PKEY{i}', 'Dor', 'David', 'Agam')")
        ims_test.ims_db.connection.commit()
    for i in range(11, 15):
        ims_test.ims_db.cursor.execute(
            f"INSERT  INTO \"DVPCB\".\"PARENT\" (ROOT_ROOTID, PKEY, FIRST_NAME, MIDDLE_NAME, LAST_NAME)"
            f"VALUES('ROOT000PC4', 'PKEY{i}', 'Shai', 'Shalo', 'Agam')")
        ims_test.ims_db.connection.commit()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC1', 'PKEY11', 'CKEY5', 'Israel', 'Lod', 'Hubert Humphry 3', '3556', '21340')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC1', 'PKEY11', 'CKEY6', 'United States', 'Chicago', 'Wacker Dr', '77', '60601')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC1', 'PKEY11', 'CKEY7', 'Japan', 'Tokyo', 'Shibuya 1-2', '109', '1500042')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC1', 'PKEY11', 'CKEY8', 'United Kingdom', 'London', 'Baker Street', '221', '60601')")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC2', 'PKEY12', 'CKEY5', 'Israel', 'Lod', 'Hubert Humphry 3', '3556', '21340')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC2', 'PKEY12', 'CKEY6', 'United States', 'New York', 'Main St 1A', '1001', '10001')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC2', 'PKEY12', 'CKEY7', 'Germany', 'Berlin', 'Unter den Linden 7', '45', '10117')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC2', 'PKEY12', 'CKEY8', 'Canada', 'Toronto', 'Queen St W 15', '222', '2222')")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC3', 'PKEY13', 'CKEY5', 'Israel', 'Lod', 'Hubert Humphry 3', '3556', '21340')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC3', 'PKEY13', 'CKEY6', 'Spain', 'Madrid', 'Gran Vía', '45', '28013')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC3', 'PKEY13', 'CKEY7', 'Brazil', 'Rio de Janeiro', 'Avenida Atlântica', '1702', '22021001')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC3', 'PKEY13', 'CKEY8', 'India', 'Mumbai', 'Bandra Kurla Complex', '101', '400051')")

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC4', 'PKEY14', 'CKEY5', 'Israel', 'Lod', 'Hubert Humphry 3', '3556', '21340')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC4', 'PKEY14', 'CKEY6', 'Ireland', 'Dublin', 'OConnell Street', '83', '11223')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC4', 'PKEY14', 'CKEY7', 'Portugal', 'Lisbon', 'Rua Augusta', '250', '1100055')")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DVPCB\".\"CHILD\" (ROOT_ROOTID, PARENT_PKEY, CKEY, COUNTRY, CITY, STREET_NAME, BUILDING_NO, ZIP_CODE) VALUES"
        "('ROOT000PC4', 'PKEY14', 'CKEY8', 'South Korea', 'Seoul', 'Gangnam-daero', '420', '06197')")

    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"ROOT\" WHERE ROOTID = 'ROOT000PC1'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"PARENT\" WHERE ROOT_ROOTID = 'ROOT000PC2'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"PARENT\" WHERE PKEY = 'PKEY3'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DVPCB\".\"CHILD\" WHERE PARENT_PKEY = 'PKEY4'")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('16', '16', '0')
    ims_test.monitor_page.delete_check('24', '18', '1')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    collect_logs(ims_test)
    finalize_test(ims_test, f"IMS_2_{ims_test.target_db.config['endpoint']}Parent_Child_Grandson")