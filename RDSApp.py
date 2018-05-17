import pandas as pd
from sqlalchemy import create_engine
import configparser

try:
    step_msg        = "Reading config file."
    config = configparser.ConfigParser()
    config.read('RDSConfig.ini')

    src_db_endpoint = config['SOURCE']['DB_ENDPOINT']
    src_db_port     = config['SOURCE']['DB_PORT']
    src_db_name     = config['SOURCE']['DB_NAME']
    src_db_usr      = config['SOURCE']['DB_USR']
    src_db_pwd      = config['SOURCE']['DB_PWD']

    trg_db_endpoint = config['TARGET']['DB_ENDPOINT']
    trg_db_port     = config['TARGET']['DB_PORT']
    trg_db_name     = config['TARGET']['DB_NAME']
    trg_db_usr      = config['TARGET']['DB_USR']
    trg_db_pwd      = config['TARGET']['DB_PWD']

    table_name      = config['DATASET']['DATASET_NAME']

    #Create connectionstrings SOURCE/TARGET
    src_engine = create_engine('postgresql+psycopg2://'+src_db_usr+':'+src_db_pwd+'@'+src_db_endpoint+':'+src_db_port+'/'+src_db_name+'',use_batch_mode=True)
    trg_engine = create_engine('postgresql+psycopg2://'+trg_db_usr+':'+trg_db_pwd+'@'+trg_db_endpoint+':'+trg_db_port+'/'+trg_db_name+'',use_batch_mode=True)

    step_msg = "Reading source dataset." #Read source
    print('Reading source dataset....') 
    src_Data = pd.read_sql('select * from "'+table_name+'" order by 1 asc', src_engine)

    step_msg = "Loading ["+table_name+"] table on target" #Loading target
    print('Loading '+table_name+' table on target....') 
    src_Data.to_sql(table_name, trg_engine,index=False,if_exists='replace',schema = "public", chunksize=1000)
    print('Table '+table_name+' loaded on target!')
    
except Exception as e:
        print("Error on process: "+step_msg +"\n Message: "+str(e))