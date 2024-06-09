from json import loads
from cache.cache import cache
from utils.clean_data import advantech_make_data, make_influx_data, imonit_clean_data
from utils.handle_data import handle_callback, make_arguments
from utils.observer import Publisher, InhandCacheObserver, CacheObserver, InfluxObserver

inhand_publisher = Publisher()
inhand_publisher.attach(InhandCacheObserver)
inhand_publisher.attach(InfluxObserver)

publisher = Publisher()
publisher.attach(CacheObserver)
publisher.attach(InfluxObserver)

def handle_data_inhand(data, schema, **kwargs):
    datas = []
    for slave, register_object in data.get("values", {}).items():
        if slave in schema.keys():
            for register, value_object in register_object.items():
                if register in schema[slave] and isinstance(value_object, dict):
                    value_object.pop("timestamp")
                    for k, v in value_object.items():
                        value_object[k] = float(v) if isinstance(v, int) else v
                    
                    influx_data = {
                        "measurement": kwargs.get("measurement"),
                        "tags": {
                            "mac_address": data.get("mac_address"),
                            "group_name": data.get("group_name"),
                            "slave": slave,
                            "reg_field": register,
                        },
                        "fields": value_object,
                    }
                    datas.append(influx_data)

    if datas:
        inhand_publisher.notify(datas, **kwargs)

def handle_data_normal(data, schema={}, **kwargs):
    record_field_keys = []

    for k, v in data.items():
        if k not in ["mac_address", "timestamp"]:
            record_field_keys.append(k)
        data[k] = float(v) if isinstance(v, int) else v

    influx_data = {
        "measurement": kwargs.get("measurement"),
        "tags": {
            "mac_address": data.get("mac_address"),
        },
        "fields": {k: v for k, v in data.items() if k in record_field_keys},
    }
    publisher.notify([influx_data], **kwargs)

def handle_data_beacon(data, schema={}, **kwargs):
    record_field_keys = []
    for k, v in data.items():
        if k not in ["mac_address", "timestamp", "type", "bleName", "rawData", "mac"]:
            record_field_keys.append(k)
        data[k] = float(v) if isinstance(v, int) else v

    influx_data = {
        "measurement": kwargs.get("measurement"),
        "tags": {
            "mac_address": data.get("mac_address"),
        },
        "fields": {k: v for k, v in data.items() if k in record_field_keys},
    }
    publisher.notify([influx_data], **kwargs)
    
def handle_data_chirpstack(data, schema={}, **kwargs):
    if data.get('object'):
        if datas:= make_influx_data(data):
            publisher.notify(datas, **kwargs)

def handle_data_temphumid(datas, schema={}, **kwargs):
    publisher.notify(datas, **kwargs)

def handle_data_imonit(data, schema={}, **kwargs):
    if datas := imonit_clean_data(data):
        publisher.notify(datas, **kwargs)

def handle_advantech(message: dict, get_params_list=[], **kwargs):
    payload = {}
    for param_object in get_params_list:
        if all(
                (arg in message.keys() or isinstance(arg, (int, float))) for arg in param_object.get('arguments', [])
            ):
            parameter = param_object.get('parameter', None)
            func = param_object.get('function', None)
            func_calback = handle_callback(func)
            arguments = param_object.get('arguments', [])
            clean_arguments = make_arguments(arguments, message)

            if parameter and func_calback:
                sensor_value = func_calback(*clean_arguments)

                if equation := param_object.get('equation', None):
                    sensor_value = eval(equation, {"value": sensor_value})

                # validate khw_total, khw_total1, khw_total2
                if parameter in ['kwh_total', 'kwh_total1', 'kwh_total2', 'flow_today', 'flow1_today', 'flow2_today', 'flow3_today']:
                    if result := cache.get(kwargs.get('mac_address')):
                        result = loads(result)
                        if result and len(result) > 0:
                            parameter_history_values = [ row.get(parameter) for row in result if row.get(parameter) ]
                            if parameter_history_values and (parameter_lasted_value := parameter_history_values[-1]) and sensor_value < parameter_lasted_value:
                                sensor_value = parameter_lasted_value
                payload[parameter] = sensor_value
    if payload:
        if datas := advantech_make_data(kwargs.get('gateway'), kwargs.get('mac_address'), payload):
            publisher.notify(datas, **kwargs)

class HandleDataField:
    brand_device = {
        "inhand": handle_data_inhand,
        "general": handle_data_normal,
        "beacon": handle_data_beacon,
        "chirpstack": handle_data_chirpstack,
        "temphumid": handle_data_temphumid,
        "imonit": handle_data_imonit,   
        "advantech": handle_advantech,
    }
    handle_func = None
    data_schema = {}
    influx_param = {}

    def __init__(self, brand, parameters, influx_param):
        if brand in self.brand_device.keys():
            self.handle_func = self.brand_device[brand]

        self.data_schema = parameters
        self.influx_param = influx_param

    def call(self, value):
        self.handle_func(value, self.data_schema, **self.influx_param)