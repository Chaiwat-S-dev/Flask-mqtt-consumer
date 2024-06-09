import pika
from time import sleep, time
from json import loads
from datetime import datetime
from influxdb_client import InfluxDBClient, WritePrecision

from influxdb_client.client.write_api import SYNCHRONOUS

from utils.config import FRASER_INFLUX_TOKEN, FRASER_INFLUX_URL, INFLUX_TOKEN, INFLUX_URL, RABBITMQ_URL, QUEUENAME

TIME_BREAKER = 691200 # 8 Hrs.

def influxdb_write_json_data(list_json_data, **kwargs) -> bool:
    organization, bucket, _, _, _ = kwargs.values()
    
    influx_url, influx_token = INFLUX_URL, INFLUX_TOKEN
    if organization == "FRASERS":
        influx_url, influx_token = FRASER_INFLUX_URL, FRASER_INFLUX_TOKEN
    
    client_influx = InfluxDBClient(url=influx_url, token=influx_token, org=organization)
    write_api = client_influx.write_api(write_options=SYNCHRONOUS)

    try:
        write_api.write(org=organization, bucket=bucket, record=list_json_data, write_precision=WritePrecision.NS)
        return True
    except Exception as e:
        print(f'[Reciever consumer write influx error]: {e=}')
        sleep(1)
        return False

def main():
    print(f'[Reciver consumer] start job: {datetime.now().isoformat()}')
    conntection_params = pika.connection.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(conntection_params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUENAME)
    insert_succeeded, insert_failed = 0, 0
    start = time()
    for method, _, body in channel.consume(queue=QUEUENAME, inactivity_timeout=60):
        if method is None:
            print(f'[Reciver consumer] waiting timeout: {datetime.now().isoformat()}')
            print(f'[Reciver consumer] result insert data: succeeded = {insert_succeeded}, failed = {insert_failed}')
            break
        content_str = body.decode("utf-8")
        content = loads(content_str)
        datas = content.get("datas", [])
        kwargs = content.get("kwargs", {})
        if (datas and kwargs) and influxdb_write_json_data(datas, **kwargs):
            channel.basic_ack(method.delivery_tag)
            insert_succeeded += 1
        else: # not call basic nack waiting for broker reque.
            insert_failed += 1
        if (time() - start) > TIME_BREAKER:
            break
    connection.close()

if __name__ == '__main__':
    main()