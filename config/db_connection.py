import pandas as pd
from db_config import config
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
from db_config import config
import inspect
import os
#========LOGGER MANAGEMENT==============
import logging
logger=logging.getLogger('raw_logger')
#========INITIALIZE DB CONNECTIONS=============
current_filename = inspect.getframeinfo(inspect.currentframe()).filename
parent_dir_filename = os.path.dirname(os.path.abspath(current_filename))
parent_proj_dir = os.path.dirname(parent_dir_filename)
# Client_cert_path = os.path.join(parent_proj_dir, 'db_certificates', '')
certi_path = r'C:\Users\Admin\Documents\sarat\Tools\Portescap-ssl'

def start_engine():#Read DB details from config file
    param_dict = config('target_db')
    try:
        param_dict = config('target_db')
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user=param_dict['user'],
            password=param_dict['password'],
            host=param_dict['host'],
            port=param_dict['port'],
            database=param_dict['database'],
        )
        ssl_args = {
            'options': '-csearch_path={}'.format('bodhee'),
            "sslmode": "require",
            "sslcert": f"{certi_path}/client.crt",
            "sslkey": f"{certi_path}/client.key",
            "sslrootcert": f"{certi_path}/ca-cert",
        }
        engine = create_engine(engine_string, connect_args=ssl_args,echo=True)
        #engine = create_engine(engine_string, echo=True)
    except Exception as e:
        logger.exception('Exception occured in Connecting to DB :%s', e)

    return engine

def read_from_db(sql_query):

    try :

        #Connect to DB and read data from DB
        engine=start_engine()
        logger.info("reading query")
        df = pd.read_sql_query(sql_query, engine)
       # engine.dispose()
    except Exception as e:
        logger.exception('Exception occured while reading data from DB :%s', e)
        df=pd.DataFrame()
    return df

def save_to_db(sql_query):

    try :

        #Connect to DB and read data from DB
        engine=start_engine()
        logger.info("reading query")
        df = pd.read_sql_query(sql_query, engine)
       # engine.dispose()
    except Exception as e:
        logger.exception('Exception occured while reading data from DB :%s', e)
        df=pd.DataFrame()
    return df