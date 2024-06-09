import json
from datetime import datetime
from typing import List, Dict
from abc import ABC, abstractmethod
from utils.struct import device_topic_struct
from cache.cache import cache
from utils.clean_data import temphumid_make_data

class Strategy(ABC):
    @staticmethod
    def handle_general_data(data, topic):
        if (mac_address := data.get('mac_address', None)) and \
            (topic in device_topic_struct.group_topic_list) and \
            (mac_address in device_topic_struct.group_topic_list[topic]):
            handle_obj = device_topic_struct.map_mac_func[mac_address]
            handle_obj.call(data)
    
    @abstractmethod
    def do_algorithm(self, data, topic: str) -> None:
        pass

class Context():
    def __init__(self, strategy: Strategy) -> None:
        self._strategy = strategy

    @property
    def strategy(self) -> Strategy:
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: Strategy) -> None:
        self._strategy = strategy

    def handle_data(self, data, topic: str) -> None:
        self._strategy.do_algorithm(data, topic)


class ImonitStrategy(Strategy):
    """
    example message = {
                        "gatewayMessage": {
                            "gatewayID": "958985",
                            "gatewayName": "Ethernet Gateway 4 - 958985",
                            "accountID": "40753",
                            "networkID": "67685",
                            "messageType": "0",
                            "power": "0",
                            "batteryLevel": "101",
                            "date": "2023-07-21 03:02:30",
                            "count": "1",
                            "signalStrength": "0",
                            "pendingChange": "False"
                        },
                        "sensorMessages": [
                            {
                                "sensorID": "711352",
                                "sensorName": "Vibrate: SCHP No.4 - 711352",
                                "applicationID": "95",
                                "networkID": "67685",
                                "dataMessageGUID": "ccf1ae81-f74b-4f1e-b191-87feb8dedc34",
                                "state": "0",
                                "messageDate": "2023-07-21 03:02:34",
                                "rawData": "7.3%7c9.5%7c8.7%7c44%7c39%7c56%7c100%7c0",
                                "dataType": "Speed|Speed|Speed|Frequency|Frequency|Frequency|Percentage",
                                "dataValue": "7.3|9.5|8.7|44|39|56|100",
                                "plotValues": "7.3|9.5|8.7|44|39|56|100",
                                "plotLabels": "X-Axis Speed|Y-Axis Speed|Z-Axis Speed|X-Axis Frequency|Y-Axis Frequency|Z-Axis Frequency|Duty Cycle",
                                "batteryLevel": "100",
                                "signalStrength": "85",
                                "pendingChange": "True",
                                "voltage": "2.83"
                            }
                        ]
                    }
    """
    def do_algorithm(self, data: Dict, topic: str) -> None:
        for sensor_obj in data.get('sensorMessages', []):
            sensor_obj.update({"mac_address", sensor_obj.get('sensorID', "")})
            
            if gateway_detail := data.get('gatewayMessage'):
                sensor_obj.update({
                    "gateway_id": gateway_detail.get("gatewayID", None),
                    "network_id": gateway_detail.get("networkID", None)
                })
            self.handle_general_data(sensor_obj, topic)


class GeneralStrategy(Strategy):
    def do_algorithm(self, data: Dict, topic: str) -> None:
        self.handle_general_data(data, topic)


class ListGeneralStrategy(Strategy):
    def do_algorithm(self, data: List, topic: str) -> None:
        for obj in data:
            self.handle_general_data(obj, topic)


class ChirpstackStrategy(Strategy):
    def do_algorithm(self, data: Dict, topic: str) -> None:
        if topic in device_topic_struct.group_topic_list:

            if isinstance(data, list):
                for obj in data:
                    if mac := obj.get('devEUI', None):
                        handle_obj = device_topic_struct.map_mac_func[mac]
                        handle_obj.call(obj)
            else:
                if mac := data.get('devEUI', None):
                    handle_obj = device_topic_struct.map_mac_func[mac]
                    handle_obj.call(data)


class AdvantechStrategy(Strategy):
    def do_algorithm(self, data: Dict, topic: str) -> None:
        mac_address = topic.split('/')[2]

        if (topic in device_topic_struct.group_topic_list) and (mac_address in device_topic_struct.group_topic_list[topic]):
            handle_obj = device_topic_struct.map_mac_func[mac_address]
            handle_obj.call(data)


class ProjectDKSHStrategy(Strategy):
    def do_algorithm(self, data: List, topic: str) -> None:
        gateway = ""
        datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        data_list = []
        for obj in data:
            # for brand is temphumid
            if mac := obj.get('mac', None):
                if obj.get('type') == "Gateway":
                    gateway = mac
                    continue

                if (topic in device_topic_struct.group_topic_list) and (mac in device_topic_struct.group_topic_list[topic]):
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


class ProjectTOPStrategy(Strategy):
    def do_algorithm(self, data: List, topic: str) -> None:
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

                if (topic in device_topic_struct.group_topic_list) and (mac in device_topic_struct.group_topic_list[topic]):
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
                    cache.delete(f'{mac_address}-pending')

                # data have some fields & wait 
                else:
                    redis_fields = cache.get(f'{mac_address}-pending')
                    if redis_fields:
                        get_redis_fields = json.loads(redis_fields)
                        get_redis_fields.update(data['fields'])
                        if all(key in get_redis_fields for key in sensitive_key):
                            data['fields'].update(get_redis_fields)
                            data_list.append(data)
                            cache.delete(f'{mac_address}-pending')
                        else:
                            cache.set(f'{mac_address}-pending', json.dumps(get_redis_fields))
                    else:
                        cache.set(f'{mac_address}-pending', json.dumps(data['fields']))
            
            handle_obj.call(data_list)