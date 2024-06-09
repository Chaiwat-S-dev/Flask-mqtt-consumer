import pika
from json import dumps
from time import sleep
from datetime import datetime

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from utils.config import INFLUX_URL, INFLUX_TOKEN, FRASER_INFLUX_URL, FRASER_INFLUX_TOKEN, RABBITMQ_URL, QUEUENAME, DEBUG

from logger.logging import SingletonLogger

log = SingletonLogger.get_logger_instance().logger
prop = pika.BasicProperties(
            content_type='application/json',
            content_encoding='utf-8',
            headers={'key': 'value'},
            delivery_mode = pika.DeliveryMode.Persistent,
        )

def influxdb_write_json_data(list_json_data: list, **kwargs):
    organization, bucket, _, _, _ = kwargs.values()
    
    influx_url, influx_token = INFLUX_URL, INFLUX_TOKEN
    if organization == "FRASERS":
        influx_url, influx_token = FRASER_INFLUX_URL, FRASER_INFLUX_TOKEN
    
    client_influx = InfluxDBClient(url=influx_url, token=influx_token, org=organization)
    write_api = client_influx.write_api(write_options=SYNCHRONOUS)

    for row in list_json_data:
        row.update({"time": datetime.utcnow().replace(microsecond=0).isoformat()})

    if DEBUG:
        log.info(f'[Data Topic]: data to line protocol: {list_json_data}')
    try:
        write_api.write(org=organization, bucket=bucket, record=list_json_data, write_precision=WritePrecision.NS)
    except Exception as e:
        conntection_params = pika.connection.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(conntection_params)
        body_message = {"datas": list_json_data, "kwargs": kwargs}
        channel = connection.channel()
        channel.queue_declare(queue=QUEUENAME)
        channel.basic_publish(
                            exchange='',
                            routing_key=QUEUENAME,
                            properties=prop,
                            body=dumps(body_message)
                        )
        connection.close()
        log.error(f'[Influx error]: {e=}')
        sleep(1)