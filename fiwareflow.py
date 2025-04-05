"""
Just your simple class for connection to FIWARE platform including IoT Agent UL, Mosquitto Broker, QuantumLeap and CrateDB.
"""

#Import modules
import json
import http.client
import pandas as pd
from datetime import datetime
import time

class fiwareClient():
    def __init__(self, host=None, OCB_port=1026, fiware_service='test', fiware_service_path='/', timeout = 10):
        """
        Class constructor.
            Args:
                host (str):                 Fiware server address (e.g., '192.168.0.164').
                OCB_port (int):             Port of the orion context broker service.
                fiware_service (str):       Header defined so that entities for a given service can be held in a separate mongoDB database.
                fiware_service_path (str):  Path used to differentiate between arrays of devices.       
                timeout (int):              Timeout for connections in seconds.
        """
        #Compulsory input check
        if (host is None):
            print('Error in __init__: Required address of the FIWARE services')
            return
        
        #Set of default values for attributes
        self.fiware_service=fiware_service
        self.fiware_service_path=fiware_service_path
        self.OCB_port=OCB_port
        self.timeout=timeout
        self.host=host

        return
    
    def check_OCB_health(self):
        """
        This method checks the health of the Orion Context Broker service
            Returns:
                int: Not alive - 0, Alive - 1 or Connection error - 2.
        """
        #Building the request
        conn = http.client.HTTPConnection(self.host, str(self.OCB_port), timeout = self.timeout)
        try:
            conn.request("GET","/version")      
            response = conn.getresponse()
            conn.close()
            #Return the results
            if (response.status != 200):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 2
        
    def check_IoTAgentUL_health(self, IoTAUL_port=4061):
        """
        This method checks the health of the IoT Agent UL service.
            Args:
                IoTAUL_port (int): Port of the IoT Agent UL service.

            Returns:
                int: Not alive - 0, Alive - 1 or Connection error - 2.
        """

        #Building the request
        conn = http.client.HTTPConnection(self.host, str(IoTAUL_port), timeout = self.timeout)
        try:
            conn.request("GET","/iot/about")      
            response = conn.getresponse()
            conn.close()
            #Return the results
            if response.status != 200:
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 2

    def create_entity(self, entity):
        """
        This method creates a custom entity.

            Args:
                entity (dict):  Entity to be created. Id and type attributes are mandatory.
            Returns:
                int: Error - 0, Success - 1.
        """
        #Building header
        headers =   {
                        "Content-Type":"application/json",
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }
    
        #Building body
        body = json.dumps(entity)

        #Building request
        conn = http.client.HTTPConnection(self.host, str(self.OCB_port), timeout = self.timeout)
        try:   
            conn.request("POST","/v2/entities", body = body, headers = headers)
            response = conn.getresponse()
            conn.close()
            if (response.status != 201):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 0
        
    def get_entities(self):
        """
        This method gets the entities in a certain fiware_service and fiware_service_path.
            Returns:
                list: Dictionaries with entities' details or ["0"] in case of error.
        """
        #Building header
        headers =   {   
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        #Building request
        conn = http.client.HTTPConnection(self.host, str(self.OCB_port), timeout = self.timeout)
        #More bout the parameter limit here:
        #https://fiware-orion.readthedocs.io/en/master/orion-api.html#pagination
        try:
            conn.request("GET","/v2/entities?limit=1000", headers = headers)
            response = conn.getresponse()
            body = response.read()
            conn.close()
            entities = json.loads(body)
            return entities
        except Exception as e:
            print(e)
            return ["0"]
        
    def delete_entity(self, entity_id):
        """
        This method deletes an entity in a certain fiware_service and fiware_service_path.
            Args:
                entity_id (str): Id of the entity to be deleted.

            Returns:
                int: Error - 0, Success - 1.
        """
        #Building header
        headers =   {
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }
        #Building request
        conn = http.client.HTTPConnection(self.host,str(self.OCB_port),timeout = self.timeout)
        try:
            conn.request("DELETE","/v2/entities/" + entity_id, headers = headers)
            response = conn.getresponse()
            conn.close()
            if (response.status != 204):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 0

    def update_entity_attrs(self, entity_updated):
        """
        This method update the value of all attributes of a given entity.
            Args:
                entity_updated (dict):  Entity with their attributes updated. Id and type attributes are mandatory.
                    
            Returns:
                int: Error - 0, Success - 1.
        """
        #Building header
        headers =   {
                        "Content-Type":"application/json",
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        try:   
            #Eliminate id, type and metadata keys
            entity_updated_copy = entity_updated.copy()
            entity_updated_copy.pop("id")
            entity_updated_copy.pop("type")
            for key in entity_updated_copy.keys():
                entity_updated_copy[key].pop("metadata")
            body = json.dumps(entity_updated_copy)

            print("/v2/entities/" + entity_updated["id"] + "/attrs?type=" + entity_updated["type"])

            #Building request
            conn = http.client.HTTPConnection(self.host,str(self.OCB_port), timeout = self.timeout)
            conn.request("PATCH","/v2/entities/" + entity_updated["id"] + "/attrs?type=" + entity_updated["type"], body = body, headers = headers)
            response = conn.getresponse()
            conn.close()

            if (response.status != 204):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 0
        
    def create_service(self, apikey, entity_type, IoTAUL_port=4061, resource='/iot/d'):
        """
        This method creates a group to be used for the devices.
            Args: 
                apikey (str):         Password to be used for the devices.
                entity_type (str):    Type of the entities that will use this group.
                IoTAUL_port (int):    Port of the IoT Agent UL service.
                resource (str):       Endpoint for devices.
            Returns:
                int: Error - 0, Success - 1.
        """
        #Building headers and body
        body =  {
                "services":
                    [
                        {
                            "apikey":apikey, 
                            "cbroker":"http://orion:"+str(self.OCB_port),
                            "entity_type":entity_type,
                            "resource": resource
                        }
                    ]
                }
        headers =   {
                        "Content-Type":"application/json",
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }
    
        #Building the request
        conn = http.client.HTTPConnection(self.host,str(IoTAUL_port),timeout = self.timeout)
        try:
            conn.request("POST","/iot/services", body = json.dumps(body), headers = headers)      
            response = conn.getresponse()
            if (response.status != 201):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 0

    def get_services(self, IoTAUL_port=4061):
        """
        This method gets the active services in a certain fiware_service and fiware_service_path.
            Args:
                IoTAUL_port (int):    Port of the IoT Agent UL service.
            Returns:
                list: Dictionaries with services' details or ["0"] in case of error.
        """
        #Building header
        headers =   {
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        #Building request
        #More bout the parameter limit here:
        #https://fiware-orion.readthedocs.io/en/master/orion-api.html#pagination
        conn = http.client.HTTPConnection(self.host, str(IoTAUL_port), timeout = self.timeout)
        try:
            conn.request("GET","/iot/services?limit=1000", headers = headers)
            response = conn.getresponse()
            body = response.read()
            conn.close()
            services = json.loads(body)
            return services["services"]
        except Exception as e:
            print(e)
            return ["0"]
        

    def delete_service(self, apikey, IoTAUL_port=4061, resource='/iot/d'):
        """
        This method deletes an active service in a certain fiware_service and fiware_service_path.
            Args:
                apikey (str):       Password used by the devices of the service.
                IoTAUL_port (int):  Port of the IoT Agent UL service.
                resource (str):     Endpoint for devices.
            Returns:
                int: Error - 0, Success - 1.
        """

        #Building header
        headers =   {
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        #Building request
        conn = http.client.HTTPConnection(self.host, str(IoTAUL_port), timeout = self.timeout)
        try:
            conn.request("DELETE","/iot/services/?resource="+resource+"&apikey="+apikey, headers = headers)
            response = conn.getresponse()
            conn.close()
            if (response.status != 204):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 0
        

    def create_HTTP_device(self, device_id, entity, timezone, IoTAUL_port=4061):
        """
        This method creates an HTTP device in a certain fiware_service and fiware_service_path.
            Args:
                device_id (str):      Id of the device created.
                entity (dict):        Entity whose attributes will be modified by the created device.
                timezone (str):       Timezone of the created device (e.g., "Europe/Madrid").
                IoTAUL_port (int):    Port of the IoT Agent UL service.
            Returns:
                int: Error - 0, Success - 1
        """
        #Building header
        headers =   {
                        "Content-Type":"application/json",
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        #Get entity id, type and attributes
        entityCopy = entity.copy()
        entity_name = entityCopy["id"]
        entity_type = entityCopy["type"]
        entityCopy.pop("id")
        entityCopy.pop("type")
        attribute_keys = entityCopy.keys()
        attributes = []
        for attr in attribute_keys:
            attributes.append   ({  "object_id": attr, 
                                    "name": attr, 
                                    "type": entityCopy[attr]["type"]
                                })
                            
        #Building body
        body =  {
                "devices":
                    [
                        {
                            "device_id": device_id,
                            "entity_name": entity_name,
                            "entity_type": entity_type,
                            #"protocol": "PDI-IoTA-UltraLight",
                            #"transport": "HTTP",
                            "timezone": timezone,
                            "Attributes": attributes
                        }
                    ]
                }

        #Building request
        conn = http.client.HTTPConnection(self.host, str(IoTAUL_port), timeout = self.timeout)
        try:
            conn.request("POST","/iot/devices/", body = json.dumps(body), headers = headers)
            response = conn.getresponse()
            conn.close()
            if (response.status != 201):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 0
        
    def get_HTTP_devices(self, IoTAUL_port=4061):
        """
        This method gets the active HTTP devices in a certain fiware_service and fiware_service_path.
            Args:
                IoTAUL_port (int):    Port of the IoT Agent UL service.
            Returns:
                list: Dictionaries with HTTP devices' details or ["0"] in case of error.
        """
        #Building header
        headers =   {
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        #Building request
        #More bout the parameter limit here:
        #https://fiware-orion.readthedocs.io/en/master/orion-api.html#pagination
        conn = http.client.HTTPConnection(self.host, str(IoTAUL_port), timeout = self.timeout)
        try:
            conn.request("GET","/iot/devices?limit=1000", headers = headers)
            response = conn.getresponse()
            body = response.read()
            conn.close()
            devices = json.loads(body)
            return devices["devices"]
        except Exception as e:
            print(e)
            return ["0"]
        
    def delete_HTTP_device(self, device_id, IoTAUL_port=4061):
        """
        This method deletes an active HTTP device in a certain fiware_service and fiware_service_path.
            Args:
                device_id (str):      Id of the device to be deleted.
                IoTAUL_port (int):    Port of the IoT Agent UL service.
            Returns:
                int: Error - 0, Success - 1.
        """
        #Building header
        headers =   {
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        #Building request
        conn = http.client.HTTPConnection(self.host,str(IoTAUL_port),timeout = self.timeout)
        try:
            conn.request("DELETE","/iot/devices/" + str(device_id), headers = headers)
            response = conn.getresponse()
            conn.close()
            if (response.status != 204):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 0

    def create_MQTT_device(self, device_id, entity, timezone, IoTAUL_port=4061):
        """
        This method creates an MQTT device in a certain fiware_service and fiware_service_path.
            Args:
                device_id (str):    Id of the device created.
                entity (dict):      Entity whose attributes will be modified by the created device.
                timezone (str):     Timezone of the created device (e.g., "Europe/Madrid").
                IoTAUL_port (int):  Port of the IoT Agent UL service.
            Returns:
                int: Error - 0, Success - 1
        """
        #Building header
        headers =   {
                        "Content-Type":"application/json",
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        #Get entity id, type and attributes
        entityCopy = entity.copy()
        entity_name = entityCopy["id"]
        entity_type = entityCopy["type"]
        entityCopy.pop("id")
        entityCopy.pop("type")
        attribute_keys = entityCopy.keys()
        attributes = []
        for attr in attribute_keys:
            attributes.append   ({  "object_id": attr, 
                                    "name": attr, 
                                    "type": entityCopy[attr]["type"]
                                })

        #Building body
        body =  {
                "devices":
                    [
                        {
                            "device_id": device_id,
                            "entity_name": entity_name,
                            "entity_type": entity_type,
                            "protocol": "PDI-IoTA-UltraLight",
                            "transport": "MQTT",
                            #"timezone": "Europe/Madrid",
                            "timezone": timezone,
                            "Attributes": attributes
                        }
                    ]
                }

        #Building request
        conn = http.client.HTTPConnection(self.host, str(IoTAUL_port),timeout = self.timeout)
        try:
            conn.request("POST","/iot/devices/", body = json.dumps(body), headers = headers)
            response = conn.getresponse()
            conn.close()
            if (response.status != 201):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 0


    def get_MQTT_devices(self, IoTAUL_port=4061):
        """
        This method gets the active MQTT devices in a certain fiware_service and fiware_service_path.
            Args:
                IoTAUL_port (int):    Port of the IoT Agent UL service.
            Returns:
                list: Dictionaries with MQTT devices' details or ["0"] in case of error.
        """
        #Building header
        headers =   {
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        #Building request
        #More bout the parameter limit here:
        #https://fiware-orion.readthedocs.io/en/master/orion-api.html#pagination
        conn = http.client.HTTPConnection(self.host, str(IoTAUL_port),timeout = self.timeout)
        try:
            conn.request("GET","/iot/devices?limit=1000", headers = headers)
            response = conn.getresponse()
            body = response.read()
            conn.close()
            devices = json.loads(body)
            return devices["devices"]
        except Exception as e:
            print(e)
            return ["0"]

    def delete_MQTT_device(self, device_id, IoTAUL_port=4061):
        """
        This method deletes an active MQTT device in a certain fiware_service and fiware_service_path.
            Args:
                device_id (str):  Id of the device to be deleted.
                IoTAUL_port (int):    Port of the IoT Agent UL service.
            Returns:
                int: Error - 0, Success - 1.
        """
        #Building header
        headers =   {
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        #Building request
        conn = http.client.HTTPConnection(self.host, str(IoTAUL_port),timeout = self.timeout)
        try:
            conn.request("DELETE","/iot/devices/" + str(device_id), headers = headers)
            response = conn.getresponse()
            conn.close()
            if (response.status != 204):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 0
        
    def create_quantumleap_subscription(self, entity_type, QL_port=8668):
        """
        This method creates a subscription to inform quantumleap directly of context changes in entities located at a certain fiware_service and fiware_service_path.
            Args: 
                entity_type (str):    Entity type to which quantumleap is going to be subscribed.
                QL_port (int):        Port of the QuantumLeap service.
            Returns:
                int: Error - 0, Success - 1.
        """
        #Building header
        headers =   {
                        "Content-Type":"application/json",
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        #Building body
        #INFO: https://github.com/telefonicaid/fiware-orion/blob/master/doc/manuals/orion-api.md#subscription-payload-datamodel
        body =  {
                    "description": "Notify QuantumLeap of context changes in any" + entity_type + " entity",
                    "subject":  
                                {
                                    "entities": 
                                                [
                                                    {
                                                        "idPattern": ".*",
                                                        "type": entity_type
                                                    }
                                                ]
                                },
                    "notification": 
                                    {
                                        "http": 
                                                {
                                                    "url": "http://quantumleap:"+str(QL_port)+"/v2/notify"
                                                },
                                        "attrs": [], #It means that all attributes are notified
                                        "metadata": 
                                                    [
                                                        "dateCreated",
                                                        "dateModified"
                                                    ]
                                    }
                }

        #Building request
        conn = http.client.HTTPConnection(self.host,str(self.OCB_port),timeout = self.timeout)
        try:
            conn.request("POST","/v2/subscriptions/", body = json.dumps(body), headers = headers)
            response = conn.getresponse()
            conn.close()
            if (response.status != 201):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 0

    def get_quantumleap_subscriptions(self):
        """
        This method gets the active quantumleap subscriptions in a certain fiware_service and fiware_service_path.
            Returns:
                list: Dictionaries with subscriptions' details or ["0"] in case of error.
        """
        #Building header
        headers =   {
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }

        #Building request
        #More bout the parameter limit here:
        #https://fiware-orion.readthedocs.io/en/master/orion-api.html#pagination
        conn = http.client.HTTPConnection(self.host, str(self.OCB_port), timeout = self.timeout)
        try:
            conn.request("GET","/v2/subscriptions?limit=1000", headers = headers)
            response = conn.getresponse()
            body = response.read()
            conn.close()
            subscriptions = json.loads(body)
            return subscriptions
        except Exception as e:
            print(e)
            return ["0"]

    def delete_quantumleap_subscription(self, subscription_id):
        """
        This method deletes an active quantumleap subscription in a certain fiware_service and fiware_service_path.
            Args:
                subscription_id (str):    Id of the quantumleap subscription to be deleted.
            Returns:
                int: Error - 0, Success - 1.
        """
        #Building header
        headers =   {
                        "fiware-service":self.fiware_service,
                        "fiware-servicepath":self.fiware_service_path
                    }
 
        #Building request
        conn = http.client.HTTPConnection(self.host, str(self.OCB_port), timeout = self.timeout)
        try:
            conn.request("DELETE","/v2/subscriptions/" + str(subscription_id), headers = headers)
            response = conn.getresponse()
            conn.close()
            if (response.status != 204):
                return 0
            else:
                return 1
        except Exception as e:
            print(e)
            return 0
        

    def delete_all_entities(self):
        """
        This method deletes all entities in a certain fiware_service and fiware_service_path.
            Returns:
                int: Error - 0, Success - 1.
        """
        #Get the entities as a list
        entities = self.get_entities() 
        if(entities==["0"]):
            return 0   
        result = len(entities)

        #Delete each entity
        for entity in entities:
            result -= self.delete_entity(entity_id = entity["id"])
    
        #Success if and only if all entities were deleted
        if(result==0):
            return 1
        else:
            return 0
        

    def delete_all_services(self, IoTAUL_port=4061):
        """
        This method deletes all services in a certain fiware_service and fiware_service_path.
            Args:
                IoTAUL_port (int):    Port of the IoT Agent UL service.
            Returns:
                int: Error - 0, Success - 1.
        """
        #Get the services as a list
        services = self.get_services()
        if(services==["0"]):
            return 0  
        result = len(services)

        #Delete each service
        for service in services:
            result -= self.delete_service(apikey = service["apikey"], IoTAUL_port=IoTAUL_port)
    
        #Success if and only if all services were deleted
        if(result==0):
            return 1
        else:
            return 0

    def delete_all_MQTT_devices(self, IoTAUL_port=4061):
        """
        This method deletes all MQTT devices in a certain fiware_service and fiware_service_path.
            Args:
                IoTAUL_port (int):    Port of the IoT Agent UL service.
            Returns:
                int: Error - 0, Success - 1.
        """
        #Get the devices as a list
        devices = self.get_MQTT_devices()
        if(devices==["0"]):
            return 0 
        result = len(devices)

        #Delete each device
        for device in devices:
            result -= self.delete_MQTT_device(device_id = device["device_id"], IoTAUL_port=IoTAUL_port)
        #Success if and only if all devices were deleted
        if(result==0):
            return 1
        else:
            return 0

    def delete_all_quantumleap_subscriptions(self):
        """
        This method deletes all quantumleap subscriptions in a certain fiware_service and fiware_service_path.
            Returns:
                int: Error - 0, Success - 1.
        """
        #Get the quantumleap subscriptions as a list
        subscriptions = self.get_quantumleap_subscriptions()
        if(subscriptions==["0"]):
            return 0 
        result = len(subscriptions)

        #Delete each quantumleap subscription
        for subscription in subscriptions:
            result -= self.delete_quantumleap_subscription(subscription_id = subscription["id"])
    
        #Success if and only if all quantumleap subscription were deleted
        if(result==0):
            return 1
        else:
            return 0
        
    def get_data_CrateDB(self, entity_id, database, from_date, to_date, limit, crateDB_port=4200):
        """
        This method retrieves the last "limit" values between dates of the attributes for a certain entity.
            Args:
                entity_id (str):      Id of the entity to be retrieved or '' to retrieve values of all entities.
                database (str):       Name of the crateDB database that holds the data.
                from_date (str):      Beginning of the time range in ISO8601 format and GMT time (e.g.,2018-01-05T15:44:34).
                to_date (str):        End of the time range in ISO8601 format and GMT time (e.g.,2018-01-05T15:44:34).
                limit (int):          Maximum number of values to be retrieved.
                crateDB_port (int):   Port of the CrateDB data base.
            Returns:
                dict: Where key "cols" is the name of the columns and "rows" contains the data as a list, ["0"] in case of error.
        """
        #Building header
        headers = {"Content-Type": 'application/json'}

        #Compute the timestamps
        fromTimestamp = datetime.timestamp(datetime.strptime(from_date, '%Y-%m-%dT%H:%M:%S'))
        toTimestamp = datetime.timestamp(datetime.strptime(to_date, '%Y-%m-%dT%H:%M:%S'))

        #Building the body
        if(entity_id == ''):
            body = {"stmt": f"SELECT * FROM {database} WHERE time_index >= {int(1000*fromTimestamp)} AND time_index < {int(1000*toTimestamp)} ORDER BY time_index ASC LIMIT {str(int(limit))}"}
        else:
            body = {"stmt": f"SELECT * FROM {database} WHERE entity_id = '{entity_id}' AND time_index >= {int(1000*fromTimestamp)} AND time_index < {int(1000*toTimestamp)} ORDER BY time_index ASC LIMIT {str(int(limit))}"}

        #Building request
        conn = http.client.HTTPConnection(self.host, str(crateDB_port), timeout = self.timeout)
        try:
            conn.request("POST","/_sql", body = json.dumps(body), headers = headers)
            response = conn.getresponse()
            body = json.loads(response.read())
            conn.close()
            if(response.status == 200):
                return body
            else:
                return ["0"]
        except Exception as e:
            print(e)
            return ["0"]
        

    def get_CSV_CrateDB(self, entity_id, database, from_date, to_date, limit, to_drop=None, crateDB_port=4200):
        """
        This method exports to a .csv file the last "limit" values of the attributes for a certain entity between dates
            Args:
                entity_id (str):        Id of the entity to be retrieved, '' to retrieve values of all entities.
                database (str):         Name of the crate database that holds the data.
                from_date (str):        Beginning of the time range in ISO8601 format and GMT time (e.g.,2018-01-05T15:44:34).
                to_date (str):          End of the time range in ISO8601 format and GMT time (e.g.,2018-01-05T15:44:34).
                limit (int):            Maximum number of values to be retrieved.
                to_drop (list of str):  Columns to be dropped before exporting to csv in order to optimize the space.
                crateDB_port (int):     Port of the CrateDB data base.
            Returns:
                int: Error - 0, Success - 1.
        """
        try:
            body = self.get_data_CrateDB(entity_id = entity_id, database = database, from_date = from_date, to_date = to_date, limit = limit, crateDB_port=crateDB_port)
        
            #Check the previous result
            if (body != ["0"]):
                #Retrieve data from the body of the response
                data = body["rows"]
                columns = body["cols"]

                #Building the dataframe and rename the columns
                df = pd.DataFrame(data)
                df.columns = columns

                #Delete the indicated columns if specified
                if(to_drop != None):
                    df = df.drop(columns=to_drop,errors='ignore')

                #Export to .csv
                if(entity_id == ''):
                    df.to_csv('all_from' + from_date.replace(':','-') + '_to' + to_date.replace(':','-') + '_of_' + database + '.csv', index = False, decimal = '.')
                else:
                    df.to_csv(entity_id + '_from' + from_date.replace(':','-') + '_to' + to_date.replace(':','-') + '_of_' + database + '.csv', index = False, decimal = '.')

                return 1
            else:
                return 0
        except Exception as e:
            print(e)
            return 0
        
    def insert_CSV_CrateDB(self, file, database, crateDB_port=4200):
        """
        This function inserts records from a CSV file into a certain crate database.
            Args:
                file (str):         Path of the csv file to be inserted. The file must include the column names.
                database (str):     Name of the crate database that holds the data.
                crateDB_port (int): Port of the crateDB service.
            Returns:
                int: Error - 0, Success - 1.
        """
        #Building header
        headers = {"Content-Type": 'application/json'}

        try:
            #import the data and build the dataframe
            df = pd.read_csv(file, header=0, sep = ',',dtype=object)

            #Check whether the df has the columns names
            if(list(df.columns) != []):

                #Delete the '__original_ngsi_entity__' column that contains blank spaces
                df = df.drop(columns='__original_ngsi_entity__', errors='ignore')

                #Get the size of the df
                dfSize = len(df)

                #Building the body
                #bulk_args: List of list with the values of each column
                data = []
                for i in range(0, len(df)):
                    data.append(list(df.iloc[i]))

                #stmt: Query
                columnsNames = ''
                values = ''
                for string in df.columns:
                    columnsNames += string + ', '
                    values += '?, '

                #Rows to be inserted at the same time
                sizeSubData = 1000
                j = 0
                while(j<len(data)):
                    #Select a part of the data
                    if(j+sizeSubData>len(data)):
                        subData = data[j:len(data)]
                    else:
                        subData = data[j:j+sizeSubData]

                    #Update the counter
                    j += sizeSubData

                    body =  {
                                "stmt": f"INSERT INTO {database} ({columnsNames[:-2]}) VALUES ({values[:-2]})",
                                "bulk_args": subData
                            }
            
                    #Building request
                    conn = http.client.HTTPConnection(self.host,str(crateDB_port),timeout = self.timeout)
                    conn.request("POST","/_sql", body = json.dumps(body,default=str), headers = headers)
                    response = conn.getresponse()
                    body = json.loads(response.read())
                    conn.close()

                    #Wait for 500 ms
                    time.sleep(0.5)

                    #Check the status of each row
                    for row in body['results']:
                        dfSize -= row['rowcount']

                    #Check the response code
                    if(response.status != 200):
                        return 0

                #Check whether all rows have been inserted correctly
                if(dfSize == 0):
                    return 1
                else:
                    return 0
        except:
                return 0