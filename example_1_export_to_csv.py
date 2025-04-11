"""
Description:   Script to export the attributes' history as a .csv file
"""
#Include modules
from fiwareflow import fiwareClient

# Client parameters
host = 'xxx.xxx.xxx.xxx'
fiware_service = 'test'
fiware_service_path = '/'

# File parameters
path = './Dataset.csv'
entity_id = 'room1'
database = '------------'
limit = 1e6
from_date = '2022-09-01T00:00:00'
to_date = '2028-12-01T00:00:00'
to_drop = ['__original_ngsi_entity__']

# Instance the client
client = fiwareClient(host, fiware_service, fiware_service_path)

if(client.get_CSV_CrateDB(path=path,
                          entity_id=entity_id, 
                          database=database, 
                          from_date=from_date, 
                          to_date=to_date, 
                          to_drop=to_drop, 
                          limit=limit) == 1):
    print('File exported successfully')
else:
    print('Something was wrong')