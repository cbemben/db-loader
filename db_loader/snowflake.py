import snowflake.connector
import sys
import os
import configparser
import logging
from pathlib import Path

def snowflake_connector(func):
    """ decorator function to pass Snowflake credentials and create
        connection to database. All necessary connection information
        must be present as environment variables 
    """
    def with_connection_(*args,**kwargs):
        con = snowflake.connector.connect(
        	  	user=os.environ['SNOWFLAKE_USER'],
        	  	#password=os.environ['SNOWFLAKE_PWD'],
        	  	account=os.environ['SNOWFLAKE_ACCT'],
        	  	role=os.environ['SNOWFLAKE_ROLE'],
        	  	warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        	  	database=os.environ['SNOWFLAKE_DB'],
        	  	schema=os.environ['SNOWFLAKE_SCHEMA'],
        	  	authenticator='externalbrowser'
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

def get_config_value(section: str, key: str, config_path: str = 'config.ini') -> str:
    """ Helper function to help collect and parse config values.
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    return config[section][key]

def get_list_of_files():
    """ Returns a list of individual filenames that are present in the target directory
    """
    dir_path = get_config_value(section="DirectoryPath",key="Directory")
    dir_name = get_config_value(section="FilePaths", key="SurgicalAsset")
    full_path = dir_path + dir_name
    list_of_files = list(Path(full_path).glob('*.*'))
    return list_of_files

def get_diff_list_of_files():
    """ Returns a list of files that have NOT been loaded into the target
        Snowflake named stage defined in the config file
    """
    incoming_files = [str(i) for i in get_list_of_files()]
    files_in_snowflake = get_list_of_files_in_stage()
    z = []
    for i in incoming_files:
        for j in files_in_snowflake:
            if i.endswith(j):
                z.append(i)
    scope = set(incoming_files).difference(set(z))            
    return list(scope)

@snowflake_connector
def get_list_of_files_in_stage(con):
    """ Returns a list of files in the Snowflake named stage defined
        in the config file.
    """
    snowflake_stage_name = get_config_value("SnowflakeStage","NAMED_STAGE_1")
    query = 'select distinct metadata$filename from ' + snowflake_stage_name
    print(query)
    cur = con.cursor()
    cur.execute(query)
    return list(cur.fetch_pandas_all()['METADATA$FILENAME'])

@snowflake_connector
def push_to_snowflake_stage(con, snowflake_stage_name: str = None):
    """ Moves files from the target directory into the Snowflake named stage.
    """
    list_of_files = get_diff_list_of_files()
    snowflake_stage_name = get_config_value("SnowflakeStage","NAMED_STAGE_1")
    for i in list_of_files:
        if sys.platform == 'win32':
            x = "PUT 'file://" + Path(i).as_posix() + "' " + snowflake_stage_name
        else:
            x = "PUT 'file://" + i + "' " + snowflake_stage_name
        print(x)
        con.cursor().execute(x)
