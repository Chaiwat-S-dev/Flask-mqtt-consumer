import json

from flask_mqtt import Mqtt

from mqtt.strategy import *
from logger.logging import SingletonLogger
from utils.config import DEVICE_BRAND
from utils.struct import device_topic_struct

mqtt_client = Mqtt()

log = SingletonLogger.get_logger_instance().logger

@mqtt_client.on_connect()
def handle_connect(client, userdata, flags, rc):
    if rc == 0:
        print('Connected successfully')

        for topic in device_topic_struct.topic_listen:
            mqtt_client.subscribe(topic)
    else:
        print('Bad connection. Code:', rc)


@mqtt_client.on_message()
def handle_mqtt_message(client, userdata, message):
    try:
        data = json.loads(message.payload.decode())
    except json.JSONDecodeError as e:
        log.error(f'[json decode error]: {message.payload.decode()}, {e=}')
        return
    
    topic = message.topic
    match DEVICE_BRAND:
        case 'advantech':
            context = Context(AdvantechStrategy())

        case 'chirpstack':
            context = Context(ChirpstackStrategy())

        case _:
            if isinstance(data, list):
                match topic:
                    # For Customer => "TOPS"
                    case 'swd/TOPS/demo/temphumid':
                        context = Context(ProjectTOPStrategy())

                    # For Customer => "DKSH"
                    case 'swd/DKSH/demo/temphumid':
                        context = Context(ProjectDKSHStrategy())

                    # For Other Customer
                    case _:
                        topic = "/".join(message.topic.split("/")[0:2]) + "/#"
                        context = Context(ListGeneralStrategy())

            else:
                topic = "/".join(message.topic.split("/")[0:2]) + "/#"
                if "mac_address" in data.keys():
                    context = Context(GeneralStrategy())

                # Imonit Payload Data
                elif 'sensorMessages' in data.keys():
                    context = Context(ImonitStrategy())
                else:
                    return
    context.handle_data(data, topic)