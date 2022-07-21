# db-loader
Package to lift external data into db.

Currently this process assumes there is a primary directory which contains a set of sub-directories containing the individual files to load. The Directory Path section of the config specifies the primary directory path while the File Paths section specifies the different sub-directories.

## Config Required
Create a `config.ini` file in the `db_loader` directory with the following structure.

```console
[DirectoryPath]
Directory=/path/to/the/directory/containing/sub-directories/which/contain/individual/files

[FilePaths]
subdirectory_desc_1=subdirectory_name_1/
subdirectory_desc_2=subdirectory_name_2/
subdirectory_desc_3=subdirectory_name_3/

[SnowflakeStage]
NAMED_STAGE_1= __The name of the Snowflake Named Stage including @ symbol (example: @MY_STAGE)__
```

## To Run
Once config file, Snowflake credentials/locations and named stage is in place you can execute any of the functions. The primary runner to iterate and load all files is the `push_to_snowflake_stage()` function.
