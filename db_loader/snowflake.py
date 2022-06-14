import snowflake.connector
import logging
import os
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

def get_dirs(path: str):
    """Get generator object of all sub-directories
    :param path str: Parent directory
    """
    return pathlib.Path(path).iterdir()

def get_dir_files(dir_path: str):
    file_paths_for_upload = []
    folders = Path(dir_path)
    for folder in folders.iterdir():
        if not str(folder).endswith('.DS_Store'):
            files = Path(folder)
            for file in files.iterdir():
                file_paths_for_upload.append(str(file))
    return file_paths_for_upload

def put_files_into_snowflake(con, file):
    raise NotImplementedError
    #put file into snowflake

@snowflake_connector
def recurse_over_files(con):
    raise NotImplementedError
    #x = get_dir_files
    #loop over x in put_files_into_snowflake