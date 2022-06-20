import snowflake.connector
import logging
import os
import configparser
from pathlib import Path

def snowflake_connector(func):
    def with_connection_(*args,**kwargs):
        con = snowflake.connector.connect(
        	  	user=os.environ['SNOWFLAKE_USER'],
        	  	password=os.environ['SNOWFLAKE_PWD'],
        	  	account=os.environ['SNOWFLAKE_ACCT'],
        	  	role=os.environ['SNOWFLAKE_ROLE'],
        	  	warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        	  	database=os.environ['SNOWFLAKE_DB'],
        	  	schema=os.environ['SNOWFLAKE_SCHEMA']
        	  	#authenticator='externalbrowser'
                )
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

def get_config_value(section: str, key: str) -> str:
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config[section][key]

def get_list_of_files():
    dir_path = get_config_value(section="DirectoryPath",key="Directory")
    dir_name = get_config_value(section="FilePaths", key="OncSnap")
    full_path = dir_path + dir_name
    list_of_files = list(Path(full_path).glob('*.*'))
    return [str(i) for i in list_of_files]

@snowflake_connector
def push_to_snowflake_stage(con, snowflake_stage_name: str = None):
    list_of_files = get_list_of_files()
    snowflake_stage_name = get_config_value("SnowflakeStage","NAMED_STAGE_1")
    for i in list_of_files:
        x = "PUT 'file://" + i + "' " + snowflake_stage_name
        print(x)
        con.cursor().execute(x)
