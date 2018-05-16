import pandas as pd
from sqlalchemy import create_engine
import configparser

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

src_engine = create_engine('postgresql://'+src_db_usr+':'+src_db_pwd+'@'+src_db_endpoint+':'+src_db_port+'/'+src_db_name+'')
trg_engine = create_engine('postgresql://'+trg_db_usr+':'+trg_db_pwd+'@'+trg_db_endpoint+':'+trg_db_port+'/'+trg_db_name+'')

print('Read Metadata_Country_API_6_DS2_es_csv_v2.')
dfCountry = pd.read_csv('C:\\Users\\USER\OneDrive - Care Analytics, a division of TEAM International\\Proyects\\TallerAWS\\Data\\Metadata_Country_API_6_DS2_es_csv_v2.csv')
print('Loading Metadata_Country_API_6_DS2_es_csv_v2.')
dfCountry.to_sql('Country', src_engine,index=False,if_exists='append',schema = 'public')

print('Read HDI_HumanDevelopmentIndex.')
dfHDI = pd.read_csv('C:\\Users\\USER\OneDrive - Care Analytics, a division of TEAM International\\Proyects\\TallerAWS\\Data\\HDI_HumanDevelopmentIndex.csv',encoding='utf-8')
print('Loading HDI_HumanDevelopmentIndex.')
dfHDI.to_sql('HumanDevelopmentIndex', src_engine,index=False,if_exists='append',schema = 'public')

# print('Read src table.')
# src_Data = pd.read_sql("""select * from "Country" order by 1 asc""",src_engine)
# print('Loading trg table.')
# src_Data.to_sql('Country', trg_engine,index=False,if_exists='append',schema = "public")
# print('trg table loaded.')


# insert_values = df.to_dict(orient='Country')
# insert_statement = sqlalchemy.dialects.postgresql.insert(table).values(insert_values)
# upsert_statement = insert_statement.on_conflict_do_update(
#     constraint='fact_case_pkey',
#     set_= df.to_dict(orient='dict')
# )
