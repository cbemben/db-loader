import snowflake.connector
import logging
import os

def snowflake_connector(func):
    def with_connection_(*args,**kwargs):
        con = snowflake.connector.connect(
        	  	user=os.environ['SNOWFLAKE_USER'],
        	  	#password=os.environ['SNOWFLAKE_PWD'],
        	  	account=os.environ['SNOWFLAKE_ACCT'],
        	  	role=os.environ['SNOWFLAKE_ROLE'],
        	  	warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        	  	database=os.environ['SNOWFLAKE_DB'],
        	  	schema=os.environ['SNOWFLAKE_SCHEMA'],
        	  	#authenticator='externalbrowser')
        try:
            rv = func(con, *args,**kwargs)
        except Exception:
            con.rollback()
            logging.error("Database connection error")
            raise
        else:
            con.commit()
        finally:
        	print("closing connection to snowflake")
        	con.close()
        return rv
    return with_connection_




@snowflake_connector
def test_connection(con):
	cur = con.cursor()
	print("connected to snowflake")
	cur.execute("SELECT current_version()")
	ret = cur.fetchone()
	print(ret)
