#Import fiwareflow module
from fiwareflow import fiwareClient

client = fiwareClient(host='54.75.212.61', fiware_service='DOWN', fiware_service_path='/', timeout=10)
data = client.get_CSV_CrateDB(entity_id='threePhaseSensor1', database='mtdown.etthreephasesensor', from_date='2025-01-01T00:00:00', to_date='2025-05-01T00:00:00', limit=20000)