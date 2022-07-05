import pytest
from pathlib import Path
from db_loader.snowflake import get_config_value

def test_config_directory_exists():
    path_str = get_config_value(config_path='db_loader/config.ini',
                                section='DirectoryPath', 
                                key='Directory')
    assert Path(path_str).exists()
