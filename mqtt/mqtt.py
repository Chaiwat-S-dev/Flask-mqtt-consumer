import json
from datetime import datetime

from flask_mqtt import Mqtt
from sqlalchemy import not_

from cache.cache import cache
from model.models import db, DeviceModel
from utils.clean_data import temphumid_make_data
from utils.handle_callback import HandleDataField
from logger.logging import SingletonLogger

class DeviceTopicStructure:
    topic_listen: list = []
    map_mac_func: dict = {}
    group_topic_list: dict = {}

device_topic_struct = DeviceTopicStructure()
mqtt_client = Mqtt()

log = SingletonLogger.get_logger_instance().logger


def query_device_to_topic_list(query_list:list=[]):
    group_topic_list, map_mac_func = {}, {}

    for device in query_list:
        topic = f"swd/{device.organization.code}/{device.bucket}/{device.measurement}"

        influx_param = {
            'organization': device.organization,
            'bucket': device.bucket,
            'measurement': device.measurement,
            'mac_address': device.mac_address
        }
        handle_field_obj = HandleDataField(device.brand, device.parameters, influx_param)
        map_mac_func.update({device.mac_address: handle_field_obj})
        
        # Brand in ("inhand", "beacon")
        if device.brand != 'chirpstack': 
            if list_mac := group_topic_list.get(topic, None):
                list_mac.append(device.mac_address)
            else:
                group_topic_list.update({topic: [device.mac_address]})
    
    device_topic_struct.topic_listen = list(group_topic_list.keys())
    device_topic_struct.map_mac_func = map_mac_func
    device_topic_struct.group_topic_list = group_topic_list

def initial_query_device():
    list_devices = db.session.query(DeviceModel).\
                filter(DeviceModel.is_activated == True, DeviceModel.deleted == None, not_(DeviceModel.brand.like('chirpstack')))\
                .order_by(DeviceModel.id.asc()).all()
    query_device_to_topic_list(list_devices)

@mqtt_client.on_connect()
def handle_connect(client, userdata, flags, rc):
    if rc == 0:
        print('Connected successfully')
        print(f'{device_topic_struct.topic_listen=}')
        for topic in device_topic_struct.topic_listen:
            mqtt_client.subscribe(topic)
    else:
        print('Bad connection. Code:', rc)


@mqtt_client.on_message()
def handle_mqtt_message(client, userdata, message):
    
    def general_handle_data_obj(data, mac_address, message):
        if (message.topic in device_topic_struct.group_topic_list) and (mac_address in device_topic_struct.group_topic_list[message.topic]):
            handle_obj = device_topic_struct.map_mac_func[mac_address]
            handle_obj.call(data)
    
    def handle_data_tops_project(data, message):
        gateway = ""
        datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        sensitive_key = ("battery_level", "temperature", "humidity")
        data_list = []
        group_data_object = {}
        
        for obj in data:
            # for brand is temphumid
            if mac := obj.get('mac', None):
                if obj.get('type') == "Gateway":
                    gateway = mac
                    continue

                if (message.topic in device_topic_struct.group_topic_list) and (mac in device_topic_struct.group_topic_list[message.topic]):
                    if gateway:
                        obj.update({"gateway": gateway})

                    handle_obj = device_topic_struct.map_mac_func[mac]
                    if json_data := temphumid_make_data(obj):
                        # validate mac_address and is_last_data
                        mac_address = json_data.get('tags').get('mac_address')
                        if mac_address in group_data_object:
                            get_fields = json_data.get('fields')
                            get_timestamp = datetime.strptime(json_data.get('timestamp'), datetime_format)
                            get_last_timestamp = datetime.strptime(group_data_object[mac_address]['timestamp'], datetime_format)
                            if get_timestamp > get_last_timestamp:
                                group_data_object[mac_address]["timestamp"] = json_data.get('timestamp')
                                group_data_object[mac_address]["fields"].update(get_fields)
                            
                            # check has fields
                            for field, value in get_fields.items():
                                if not group_data_object[mac_address]["fields"].get(field):
                                    group_data_object[mac_address]["fields"].update({field: value})
                        else:
                            group_data_object.update({mac_address: json_data})

        if group_data_object:
            for mac_address, data in group_data_object.items():
                get_keys = data['fields'].keys()

                # Check data have all fields
                if all(key in get_keys for key in sensitive_key):
                    data_list.append(data)
                    cache.delete(mac_address)

                # data have some fields & wait 
                else:
                    redis_fields = cache.get(mac_address)
                    if redis_fields:
                        get_redis_fields = json.loads(redis_fields)
                        get_redis_fields.update(data['fields'])
                        if all(key in get_redis_fields for key in sensitive_key):
                            data['fields'].update(get_redis_fields)
                            data_list.append(data)
                            cache.delete(mac_address)
                        else:
                            cache.set(mac_address, json.dumps(get_redis_fields))
                    else:
                        cache.set(mac_address, json.dumps(data['fields']))
            
            handle_obj.call(data_list)
    
    def handle_data_tops_dksh(data, message):
        gateway = ""
        datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        data_list = []
        for obj in data:
            # for brand is temphumid
            if mac := obj.get('mac', None):
                if obj.get('type') == "Gateway":
                    gateway = mac
                    continue

                if (message.topic in device_topic_struct.group_topic_list) and (mac in device_topic_struct.group_topic_list[message.topic]):
                    data_list.append({
                        "measurement": "temphumid",
                        "timestamp": obj.get('timestamp', datetime.utcnow().strftime(datetime_format)),
                        "tags": {
                            "mac_address": mac,
                            "gateway": gateway
                        },
                        "fields": {
                            "battery_level": float(obj.get('battery', 0.0)),
                            "temperature": float(obj.get('temperature', 0.0)),
                            "humidity": float(obj.get('humidity', 0.0))
                        }
                    })
                    handle_obj = device_topic_struct.map_mac_func[mac]
                    handle_obj.call(data_list)


    data = json.loads(message.payload.decode())
    if isinstance(data, list):
        match message.topic:
            # For Customer => "TOPS"
            case 'swd/TOPS/demo/temphumid':
                handle_data_tops_project(data, message)

            # For Customer => "DKSH"
            case 'swd/DKSH/demo/temphumid':
                handle_data_tops_dksh(data, message)

            # For Other Customer
            case _:
                for obj in data:
                    if mac_address := obj.get('mac_address', None):
                        general_handle_data_obj(data, mac_address, message)

    else:
        if mac_address := data.get('mac_address', None):
            general_handle_data_obj(data, mac_address, message)

        # Imonit Payload Data
        if sensor_message_list := data.get('sensorMessages', None):
            for sensor_obj in sensor_message_list:
                mac_address = sensor_obj.get('sensorID', "")
                
                if gateway_detail := data.get('gatewayMessage'):
                    sensor_obj.update({
                        "gateway_id": gateway_detail.get("gatewayID", None),
                        "network_id": gateway_detail.get("networkID", None)
                    })

                general_handle_data_obj(data, mac_address, message)