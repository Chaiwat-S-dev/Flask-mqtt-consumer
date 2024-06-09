from datetime import datetime
from time import sleep

from influxdb_client import InfluxDBClient, WritePrecision, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from utils.config import INFLUX_URL, INFLUX_TOKEN, DEBUG
from utils.clean_data import has_required_fields, make_influx_data, imonit_clean_data

from logger.logging import SingletonLogger

log = SingletonLogger.get_logger_instance().logger

def insert_influx(influx_data, record_tags, record_keys, **kwargs):
    organization, bucket, measurement, _ = kwargs.values()
    client_influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=organization)
    write_api = client_influx.write_api(write_options=SYNCHRONOUS)
    point = Point.from_dict(influx_data, write_precision=WritePrecision.MS,
                            record_measurement_name=measurement,
                            record_time_key="timestamp",
                            record_tag_keys=record_tags,
                            record_field_keys=record_keys
                            )
    print(f'[Data Topic]: data to line protocol: {point.to_line_protocol()}')
    if DEBUG:
        log.info(f'[Data Topic]: data to line protocol: {point.to_line_protocol()}')
    try:
        write_api.write(bucket, organization, point)
    except Exception as e:
        log.error(f'[Influx error]: {e=}')
        sleep(1)

def influxdb_write_json_data(list_json_data, **kwargs):
    organization, bucket, measurement, _ = kwargs.values()
    client_influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=organization)
    write_api = client_influx.write_api(write_options=SYNCHRONOUS)

    if DEBUG:
        log.info(f'[Data Topic]: data to line protocol: {list_json_data}')
    try:
        write_api.write(org=organization, bucket=bucket, record=list_json_data, write_precision=WritePrecision.NS)
    except Exception as e:
        log.error(f'[Influx error]: {e=}')
        sleep(1)
    

def handle_data_inhand(req, db, data={}, hierachy_level=0, **kwargs):

    if isinstance(req, dict):
        if hierachy_level == 2 and db.get("equation"):
            
            from collections import OrderedDict
            param_equation = {}
            _req = OrderedDict(sorted(req.items()))
            
            for reg, reg_val in _req.items():
                if (reg in db.keys()):
                    if isinstance(reg_val, dict):
                        for k, v in reg_val.items():
                            reg_val[k] = float(v) if isinstance(v, int) else v
                        param_equation.update({reg: reg_val.get('raw_data', 0.0)})
                        influx_data = {**data, **reg_val}
            
            influx_data = {
                **influx_data, 
                **{
                    "reg_field": list(param_equation.keys())[0], 
                    "raw_data": eval(db.get("equation"), param_equation), 
                    "timestamp": datetime.utcnow(), 
                    "mac_address": kwargs.get("mac_address")
                }
            }
            
            record_field_keys = db[reg].copy()
            record_field_keys.remove("timestamp")
            record_tag_keys=["slave", "reg_field", "mac_address", "group_name"]
            
            insert_influx(influx_data, record_tag_keys, record_field_keys, **kwargs)
            return

        for slave, slave_val in req.items():
            match hierachy_level:
                case 0:
                    if slave == "group_name":
                        data.update({"group_name": slave_val})
                    if slave in db.keys():
                        handle_data_inhand(slave_val, db[slave], data, hierachy_level+1, **kwargs)

                case 1:
                    data.update({"slave": slave})
                    if slave in db.keys():
                        handle_data_inhand(slave_val, db[slave], data, hierachy_level+1, **kwargs)

                case _:
                    if slave in db.keys():
                        if isinstance(slave_val, dict):
                            for k, v in slave_val.items():
                                slave_val[k] = float(v) if isinstance(v, int) else v
                            
                            data.update({"reg_field": slave})
                            influx_data = {
                                **data, **slave_val, 
                                    **{
                                        "timestamp": datetime.utcnow(), 
                                        "mac_address": kwargs.get("mac_address")
                                    }
                                }
                            record_field_keys = db[slave].copy()
                            record_field_keys.remove("timestamp")
                            record_tag_keys=["slave", "reg_field", "mac_address", "group_name"]

                            insert_influx(influx_data, record_tag_keys, record_field_keys, **kwargs)

def handle_data_normal(req, data={}, **kwargs):
    record_field_keys, record_tag_keys = [], ["mac_address"]

    for k, v in req.items():
        if k not in ["mac_address", "timestamp"]:
            record_field_keys.append(k)
        req[k] = float(v) if isinstance(v, int) else v

    influx_data = {**req, **{"timestamp": datetime.utcnow()}}
    insert_influx(influx_data, record_tag_keys, record_field_keys, **kwargs)

def handle_data_beacon(req, data={}, **kwargs):
    record_field_keys, record_tag_keys = [], ["mac_address"]
    for k, v in req.items():
        if k not in ["mac_address", "timestamp", "type", "bleName", "rawData", "mac"]:
            record_field_keys.append(k)
        req[k] = float(v) if isinstance(v, int) else v

    influx_data = {**req, **{"timestamp": datetime.utcnow()}}
    insert_influx(influx_data, record_tag_keys, record_field_keys, **kwargs)\
    
def handle_data_chirpstack(req, data={}, **kwargs):
    is_required_fields = has_required_fields(req)
    if is_required_fields:
        make_data = make_influx_data(req)
        influxdb_write_json_data(make_data, **kwargs)

def handle_data_temphumid(req, data={}, **kwargs):
    influxdb_write_json_data(req, **kwargs)

def handle_data_imonit(req, data={}, **kwargs):
    data = imonit_clean_data(req)
    if data:
        influxdb_write_json_data(data, **kwargs)

class HandleDataField:
    brand_device = {
        "inhand": handle_data_inhand,
        "general": handle_data_normal,
        "beacon": handle_data_beacon,
        "chirpstack": handle_data_chirpstack,
        "temphumid": handle_data_temphumid,
        "imonit": handle_data_imonit,
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