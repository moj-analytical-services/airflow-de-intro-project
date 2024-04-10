# import pydbtools as pydb

# pydb.delete_table_and_data(database="dami_intro_project", 
#                            table="intro project")
import awswrangler as wr

wr.catalog.delete_table_if_exists(database="dami_intro_project", 
                            table="people")