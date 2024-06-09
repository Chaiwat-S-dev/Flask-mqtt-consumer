from typing import List
from sqlalchemy import not_
from utils.config import DEVICE_BRAND, HOST_CHIRPSTACK_DEST
from model.models import db, DeviceModel
from utils.handle_callback import HandleDataField

class DeviceTopicStructure:
    topic_listen: list = []
    map_mac_func: dict = {}
    group_topic_list: dict = {}

device_topic_struct = DeviceTopicStructure()

def query_device_to_topic_list(query_list: List[DeviceModel]) -> None:
    group_topic_list, map_mac_func = {}, {}

    for device in query_list:
        device_context = device.to_dict
        if 'advantech' == DEVICE_BRAND:
            topic = f'Advantech/{device_context["mac_address_parent"]}/{device_context["mac_address"]}/data'
        elif "chirpstack" == DEVICE_BRAND:
            if not device_context["app_id"]:
                continue
            
            device_context["mac_address"] = device_context["mac_address"].lower()
            topic = f'application/{device_context["app_id"]}/device/{(device_context["mac_address"])}/event/up'

        else:
            topic = f'swd/{device_context["organization"]}/#'

        influx_param = {
            'organization': device_context["organization"],
            'bucket': device_context["bucket"],
            'measurement': device_context["measurement"],
            'mac_address': device_context["mac_address"],
            'gateway': device_context["mac_address_parent"],
        }

        handle_field_obj = HandleDataField(device_context["brand"], device_context["parameters"], influx_param)
        map_mac_func.update({device_context["mac_address"]: handle_field_obj})
        
        if list_mac := group_topic_list.get(topic, None):
            list_mac.append(device_context["mac_address"])
        else:
            group_topic_list.update({topic: [device_context["mac_address"]]})
    
    device_topic_struct.topic_listen = list(group_topic_list.keys())
    device_topic_struct.map_mac_func = map_mac_func
    device_topic_struct.group_topic_list = group_topic_list

def initial_query_device() -> None:

    if "advantech" == DEVICE_BRAND:
        list_devices = db.session.query(DeviceModel).\
                filter(DeviceModel.is_activated == True, DeviceModel.deleted == None, DeviceModel.brand == "advantech")\
                .order_by(DeviceModel.id.asc()).all()
        query_device_to_topic_list(list_devices)

    elif "chirpstack" == DEVICE_BRAND:
        list_devices = db.session.query(DeviceModel).\
                filter(DeviceModel.is_activated == True, DeviceModel.deleted == None, DeviceModel.brand == "chirpstack",
                       DeviceModel.mqtt_host == HOST_CHIRPSTACK_DEST)\
                .order_by(DeviceModel.id.asc()).all()
        query_device_to_topic_list(list_devices)

    else:
        list_devices = db.session.query(DeviceModel).\
                    filter(DeviceModel.is_activated == True, DeviceModel.deleted == None, not_(DeviceModel.brand.in_(["chirpstack", "advantech"])))\
                    .order_by(DeviceModel.id.asc()).all()
        query_device_to_topic_list(list_devices)