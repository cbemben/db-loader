import pytest
from db_loader.snowflake import snowflake_connector

@snowflake_connector
def test_db_connection(con):
    cur = con.cursor()
    print("connected to snowflake")
    cur.execute("SELECT current_version()")
    ret = cur.fetchone()
    assert ret