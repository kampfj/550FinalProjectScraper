import mysql.connector 
from mysql.connector import errorcode
from sqlalchemy import create_engine
import pandas as pd
import os.path
import requests
import scraper_utils
import numpy as np
from bs4 import BeautifulSoup
import pdb 
import unidecode
import scraper_utils 
import pymysql
from sqlalchemy.types import NVARCHAR


def create_connection_and_engine():
    cnx = mysql.connector.connect(
            host = "database-1.c6hvqaiygo9x.us-east-2.rds.amazonaws.com",
            user = "admin",
            password = "princetonistrash")
    print(cnx)
    cursor = cnx.cursor()

    engine = create_engine(
        "mysql+mysqlconnector://admin:princetonistrash@database-1.c6hvqaiygo9x.us-east-2.rds.amazonaws.com:3306/BASEBALL"
    )
    return engine

def push_dataframe_to_engine(df, table_name, engine, types={}):
    df.to_sql(table_name, con = engine, if_exists = 'append', index=False, dtype=types)
    
def to_sql_update(df, engine, schema, table):
    df.reset_index(inplace=True)
    sql = ''' SELECT column_name from information_schema.columns
              WHERE table_schema = '{schema}' AND table_name = '{table}' AND
                    COLUMN_KEY = 'PRI';
          '''.format(schema=schema, table=table)
    id_cols = [x[0] for x in engine.execute(sql).fetchall()]
    id_vals = [df[col_name].tolist() for col_name in id_cols]
    sql = ''' DELETE FROM {schema}.{table} WHERE 0 '''.format(schema=schema, table=table)
    for row in zip(*id_vals):
        sql_row = ' AND '.join([''' {}='{}' '''.format(n, v) for n, v in zip(id_cols, row)])
        sql += ' OR ({}) '.format(sql_row)
    engine.execute(sql)
    
    df.to_sql(table, engine, schema=schema, if_exists='append', index=False)