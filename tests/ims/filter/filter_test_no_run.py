from settings import *

def test_filter_less_or_equal(ims_test):
    """Test for adding a header in transformation"""
    create_task(ims_test)
    ims_test.ims_db.cursor.execute("DELETE FROM \"DEVPCB\".\"STRUCT2\"")
