
# 🌐 FiwareFlow

**FiwareFlow** is a Python library designed to simplify interaction with the **FIWARE** ecosystem. It enables easy management of entities, devices, services, and context data queries from the **Orion Context Broker**.

Created for developers who want to integrate FIWARE into their Smart Cities, IoT projects, and Industry 4.0 solutions in a clean and efficient way.

---

## 🚀 Features

- 📡 Direct connection to Orion Context Broker
- 🧱 Create, update, delete NGSI v2 entities
- ⚙️ Device registration via IoT Agents
- 🗂️ Support for FIWARE service and subservice headers
- 🪄 Simple object-oriented interface with built-in request handling

---

## 📦 Installation

Install from local clone or via pip:

```bash
git clone https://github.com/joaquinjgz/Fiware.git
cd Fiware
pip install -r requirements.txt
```

*Or include `fiwareflow.py` directly in your project.*

---

## 💻 Quick Usage Example

```python
from fiwareflow import FiwareClient

# Create client
client = FiwareClient(
    host="192.168.0.230",
    OCB_port=1026,
    fiware_service="smartcity",
    fiware_service_path="/sensors",
    timeout=10
)

# Create an entity
entity = {
    "id": "Device001",
    "type": "AirQualitySensor",
    "co2": {"type": "Number", "value": 412},
    "temperature": {"type": "Number", "value": 22.5}
}
client.create_entity(entity)

# Retrieve entities
result = client.get_entities()
print(result)

# Update the value of the attributes of a given entity
entity = {
    "id": "Device001",
    "type": "AirQualitySensor",
    "co2": {"type": "Number", "value": 450.2},
    "temperature": {"type": "Number", "value": 18.6}
}
client.update_entity_attrs(entity)
```

---

## 📚 Core Functions

| Method | Description |
|--------|-------------|
| `check_OCB_health()` | Checks the health of the Orion Context Broker service. |
| `create_entity(entity: dict)` | Creates a new entity in the Context Broker. |
| `get_entities()` | Retrieves entities from the context broker. |
| `update_entity_attrs(entity: dict)` | Updates the value of the attributes of a given entity. |
| `delete_entity(entity_id: str)` | Deletes an entity. |
| `create_service(apikey: str, entity_type: str, IoTAUL_port: int, resource: str)` | Creates a group to be used for the devices. |
| `create_HTTP_device(device_id: str, entity: dict, timezone: str, IoTAUL_port: int)` | Registers a new HTTP device with the IoT Agent UL. |
| `get_HTTP_devices()` | Lists all registered HTTP devices. |
| `create_MQTT_device(device_id: str, entity: dict, timezone: str, IoTAUL_port: int)` | Registers a new MQTT device with the IoT Agent UL. |
| `get_MQTT_devices()` | Lists all registered MQTT devices. |
| `create_quantumleap_subscription(entity_type: str, QL_port: int)` | Creates a subscription to inform quantumleap directly of context changes in entities. |
| `get_data_CrateDB(entity_id: str, database: str, from_date: str, to_date: str, limit: int, crateDB_port: int)` | Retrieves the last "limit" values between dates of the attributes for a certain entity. |
| `get_CSV_CrateDB(entity_id: str, database: str, from_date: str, to_date: str, limit: int, to_drop: list of str, crateDB_port: int)` | Exports to a .csv file the last "limit" values of the attributes for a certain entity between dates. |
| `insert_CSV_CrateDB(file: str, database: str, crateDB_port: int)` | Inserts records from a CSV file into a certain crate database. |
---

## ⚙️ Configuration Parameters

When creating a `FiwareFlow` client, you can provide:

- `host`: The address of your Context Broker (e.g., `192.168.0.230`)
- `OCB_port`: *(optional)* Port of the orion context broker service (default: `1026`)
- `fiware_service`: *(optional)* The FIWARE service name (default: `test`)
- `fiware_service_path`: The subservice path (default: `/`)
- `timeout`: *(optional)* Timeout for communications in seconds (default: `10`)

---

## 🤝 Contributing

Contributions are welcome!

1. Fork the project
2. Create your feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes
4. Push to the branch
5. Open a Pull Request and describe your improvement

---

## 📝 License

This project is licensed under the **MIT License**.  
See the [LICENSE](LICENSE) file for details.

---

## 👤 Author

Developed by [Joaquin Garrido-Zafra](https://github.com/joaquinjgz)  
📧 Contact: [pjoaquinjgz@gmail.com](mailto:pjoaquinjgz@gmail.com)

---

## 🧠 What is FIWARE?

[FIWARE](https://www.fiware.org/) is an open-source platform enabling the creation of smart applications across domains such as Smart Cities, Energy, Healthcare, Industry, and more. It provides a suite of reusable components known as **Generic Enablers**, with the **Context Broker** at its core — enabling real-time context data management.
