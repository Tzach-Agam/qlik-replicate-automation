from settings import *

def test_array_in_variant(ims_test):
    """Test for arrays in variant"""
    create_task(ims_test)

    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT2\"")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, DEP_ON_ALPHA, DEP_ON_BETA, DEP_ON_GAMMA)"
        "VALUES ('ROOT000002', 'KEY1', 'TZACHA', 9999, 1, 1, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA, DEP_ON_BETA, DEP_ON_GAMMA)"
        "VALUES ('ROOT000002', 'KEY2', 'TZACHA', 9999, 'Z', 2, 2, 2)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA)"
        "VALUES ('ROOT000002', 'KEY3', 'TZACHA', 9999, 'A', 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA)"
        "VALUES ('ROOT000002', 'KEY4', 'TZACHA', 9999, 'B', 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA)"
        "VALUES ('ROOT000002', 'KEY5', 'TZACHA', 9999, 'C', 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY6', 'TZACHA', 9999, 'A', 1, 'AAAAA', 1111)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2, ARRAY_ALPHA_2_ALPHA_COL1, ARRAY_ALPHA_2_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY7', 'TZACHA', 9999, 'A', 2, 'A', 1, 'AA', 11)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2, ARRAY_ALPHA_2_ALPHA_COL1, ARRAY_ALPHA_2_ALPHA_COL2, ARRAY_ALPHA_3_ALPHA_COL1, ARRAY_ALPHA_3_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY8', 'TZACHA', 9999, 'A', 3, 'A', 1, 'AA', 11, 'AAA', 111)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY9', 'TZACHA', 9999, 'A', 1, 'AAAAA', 1111)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2, ARRAY_ALPHA_2_ALPHA_COL1, ARRAY_ALPHA_2_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY10', 'TZACHA', 9999, 'A', 2, 'A', 1, 'AA', 11)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2, ARRAY_ALPHA_2_ALPHA_COL1, ARRAY_ALPHA_2_ALPHA_COL2, ARRAY_ALPHA_3_ALPHA_COL1, ARRAY_ALPHA_3_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY11', 'TZACHA', 9999, 'A', 3, 'A', 1, 'AA', 11, 'AAA', 111)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY12', 'TZACHA', 9999, 'B', 1, 'BBBBB', 2222)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2, ARRAY_BETA_2_BETA_COL1, ARRAY_BETA_2_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY13', 'TZACHA', 9999, 'B', 2, 'B', 2, 'BB', 22)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2, ARRAY_BETA_2_BETA_COL1, ARRAY_BETA_2_BETA_COL2, ARRAY_BETA_3_BETA_COL1, ARRAY_BETA_3_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY14', 'TZACHA', 9999, 'B', 3, 'B', 2, 'BB', 22, 'BBB', 222)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY15', 'TZACHA', 9999, 'B', 1, 'BBBBB', 2222)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2, ARRAY_BETA_2_BETA_COL1, ARRAY_BETA_2_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY16', 'TZACHA', 9999, 'B', 2, 'B', 1, 'BB', 11)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2, ARRAY_BETA_2_BETA_COL1, ARRAY_BETA_2_BETA_COL2, ARRAY_BETA_3_BETA_COL1, ARRAY_BETA_3_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY17', 'TZACHA', 9999, 'B', 3, 'B', 1, 'BB', 11, 'BBB', 111)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY18', 'TZACHA', 9999, 'C', 1, 'CCCCC', 3333)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2, ARRAY_GAMMA_2_GAMMA_COL1, ARRAY_GAMMA_2_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY19', 'TZACHA', 9999, 'C', 2, 'C', 3, 'CC', 33)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2, ARRAY_GAMMA_2_GAMMA_COL1, ARRAY_GAMMA_2_GAMMA_COL2, ARRAY_GAMMA_3_GAMMA_COL1, ARRAY_GAMMA_3_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY20', 'TZACHA', 9999, 'C', 3, 'C', 3, 'CC', 33, 'CCC', 333)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY21', 'TZACHA', 9999, 'C', 1, 'CCCCC', 3333)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2, ARRAY_GAMMA_2_GAMMA_COL1, ARRAY_GAMMA_2_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY22', 'TZACHA', 9999, 'C', 2, 'C', 3, 'CC', 33)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2, ARRAY_GAMMA_2_GAMMA_COL1, ARRAY_GAMMA_2_GAMMA_COL2, ARRAY_GAMMA_3_GAMMA_COL1, ARRAY_GAMMA_3_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY23', 'TZACHA', 9999, 'C', 3, 'C', 3, 'CC', 33, 'CCC', 333)")
    ims_test.ims_db.connection.commit()

    ims_test.designer_page.run_new_task()
    ims_test.monitor_page.wait_for_fl('4', 60)
    ims_test.monitor_page.cdc_tab()

    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, DEP_ON_ALPHA, DEP_ON_BETA, DEP_ON_GAMMA)"
        "VALUES ('ROOT000002', 'KEY24', 'TZACHA', 9999, 1, 1, 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA, DEP_ON_BETA, DEP_ON_GAMMA)"
        "VALUES ('ROOT000002', 'KEY25', 'TZACHA', 9999, 'Z', 2, 2, 2)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA)"
        "VALUES ('ROOT000002', 'KEY26', 'TZACHA', 9999, 'A', 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA)"
        "VALUES ('ROOT000002', 'KEY27', 'TZACHA', 9999, 'B', 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA)"
        "VALUES ('ROOT000002', 'KEY28', 'TZACHA', 9999, 'C', 1)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY29', 'TZACHA', 9999, 'A', 1, 'AAAAA', 1111)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2, ARRAY_ALPHA_2_ALPHA_COL1, ARRAY_ALPHA_2_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY30', 'TZACHA', 9999, 'A', 2, 'A', 1, 'AA', 11)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2, ARRAY_ALPHA_2_ALPHA_COL1, ARRAY_ALPHA_2_ALPHA_COL2, ARRAY_ALPHA_3_ALPHA_COL1, ARRAY_ALPHA_3_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY31', 'TZACHA', 9999, 'A', 3, 'A', 1, 'AA', 11, 'AAA', 111)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY32', 'TZACHA', 9999, 'A', 1, 'AAAAA', 1111)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2, ARRAY_ALPHA_2_ALPHA_COL1, ARRAY_ALPHA_2_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY33', 'TZACHA', 9999, 'A', 2, 'A', 1, 'AA', 11)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_ALPHA,"
        "ARRAY_ALPHA_1_ALPHA_COL1, ARRAY_ALPHA_1_ALPHA_COL2, ARRAY_ALPHA_2_ALPHA_COL1, ARRAY_ALPHA_2_ALPHA_COL2, ARRAY_ALPHA_3_ALPHA_COL1, ARRAY_ALPHA_3_ALPHA_COL2)"
        "VALUES ('ROOT000002', 'KEY34', 'TZACHA', 9999, 'A', 3, 'A', 1, 'AA', 11, 'AAA', 111)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY35', 'TZACHA', 9999, 'B', 1, 'BBBBB', 2222)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2, ARRAY_BETA_2_BETA_COL1, ARRAY_BETA_2_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY36', 'TZACHA', 9999, 'B', 2, 'B', 2, 'BB', 22)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2, ARRAY_BETA_2_BETA_COL1, ARRAY_BETA_2_BETA_COL2, ARRAY_BETA_3_BETA_COL1, ARRAY_BETA_3_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY37', 'TZACHA', 9999, 'B', 3, 'B', 2, 'BB', 22, 'BBB', 222)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY38', 'TZACHA', 9999, 'B', 1, 'BBBBB', 2222)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2, ARRAY_BETA_2_BETA_COL1, ARRAY_BETA_2_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY39', 'TZACHA', 9999, 'B', 2, 'B', 2, 'BB', 22)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_BETA,"
        "ARRAY_BETA_1_BETA_COL1, ARRAY_BETA_1_BETA_COL2, ARRAY_BETA_2_BETA_COL1, ARRAY_BETA_2_BETA_COL2, ARRAY_BETA_3_BETA_COL1, ARRAY_BETA_3_BETA_COL2)"
        "VALUES ('ROOT000002', 'KEY40', 'TZACHA', 9999, 'B', 3, 'B', 2, 'BB', 22, 'BBB', 222)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY41', 'TZACHA', 9999, 'C', 1, 'CCCCC', 3333)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2, ARRAY_GAMMA_2_GAMMA_COL1, ARRAY_GAMMA_2_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY42', 'TZACHA', 9999, 'C', 2, 'C', 3, 'CC', 33)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2, ARRAY_GAMMA_2_GAMMA_COL1, ARRAY_GAMMA_2_GAMMA_COL2, ARRAY_GAMMA_3_GAMMA_COL1, ARRAY_GAMMA_3_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY43', 'TZACHA', 9999, 'C', 3, 'C', 3, 'CC', 33, 'CCC', 333)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY44', 'TZACHA', 9999, 'C', 1, 'CCCCC', 3333)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2, ARRAY_GAMMA_2_GAMMA_COL1, ARRAY_GAMMA_2_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY45', 'TZACHA', 9999, 'C', 2, 'C', 3, 'CC', 33)")
    ims_test.ims_db.cursor.execute(
        f"INSERT INTO \"DEVPCB\".\"STRUCT2\"  (ROOT_ROOTID, SKEY, COL_CHAR, COL_DEC, VARIANT_SELECTOR, DEP_ON_GAMMA,"
        "ARRAY_GAMMA_1_GAMMA_COL1, ARRAY_GAMMA_1_GAMMA_COL2, ARRAY_GAMMA_2_GAMMA_COL1, ARRAY_GAMMA_2_GAMMA_COL2, ARRAY_GAMMA_3_GAMMA_COL1, ARRAY_GAMMA_3_GAMMA_COL2)"
        "VALUES ('ROOT000002', 'KEY46', 'TZACHA', 9999, 'C', 3, 'C', 3, 'CC', 33, 'CCC', 333)")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\"  SET VARIANT_SELECTOR = 'B', ARRAY_BETA_1_BETA_COL1 = 'UPDAT', ARRAY_BETA_1_BETA_COL2 = 9999 WHERE SKEY = 'KEY9'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\"  SET VARIANT_SELECTOR = 'C', ARRAY_GAMMA_1_GAMMA_COL1 = 'UPDAT', ARRAY_GAMMA_1_GAMMA_COL2 = 9999 WHERE SKEY = 'KEY15'")
    ims_test.ims_db.cursor.execute(
        f"UPDATE \"DEVPCB\".\"STRUCT2\"  SET VARIANT_SELECTOR = 'A', ARRAY_ALPHA_1_ALPHA_COL1 = 'UPDAT', ARRAY_ALPHA_1_ALPHA_COL2 = 9999 WHERE SKEY = 'KEY21'")

    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT2\"  WHERE SKEY = 'KEY11'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT2\"  WHERE SKEY = 'KEY17'")
    ims_test.ims_db.cursor.execute(f"DELETE FROM \"DEVPCB\".\"STRUCT2\"  WHERE SKEY = 'KEY23'")
    ims_test.ims_db.connection.commit()

    ims_test.ims_db.sync_command()

    ims_test.monitor_page.wait_for_cdc()
    ims_test.monitor_page.insert_check('14', '14', '14', '23')
    ims_test.monitor_page.update_check('0', '0', '0', '3')
    ims_test.monitor_page.delete_check('4', '4', '4', '3')
    ims_test.monitor_page.stop_task()
    ims_test.monitor_page.stop_task_wait()

    ims_test.replicate_actions.navigate_to_main_page('tasks')
    move_file_to_target_dir(ims_test.config.replicate_logs_path(), ims_test.task_logs_dir,
                            f"reptask_{ims_test.task_name}.log", ims_test.config, ims_test.replicate_actions, ims_test.task_name)
    ims_test.target_db.export_schema_data_to_csv(ims_test.target_schema,
                                                 ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_IN_VARIANT.csv")
    compare_files(ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_IN_VARIANT.good",
                  ims_test.good_files_dir + f"\\IMS_2_{ims_test.target_db.config['endpoint']}_ARRAY_IN_VARIANT.csv")