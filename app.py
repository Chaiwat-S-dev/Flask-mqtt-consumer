import ssl
from flask import Flask
from mqtt.broker_listen import MQTTHostChoice, broker_host
from mqtt.mqtt import mqtt_client
from model.models import db
from route import api
from utils.struct import initial_query_device
from utils.config import *

broker = ""
broker_port = 0
ssl_mode = False
client_id = MQTT_CLIENT_ID

match DEVICE_BRAND:
    case "chirpstack":
        match HOST_CHIRPSTACK_DEST:
            case MQTTHostChoice.CHRIPSTACK_HOST_1.value:
                broker = broker_host[HOST_CHIRPSTACK_DEST]["host"]
                broker_port = broker_host[HOST_CHIRPSTACK_DEST]["port"]
                ssl_mode = broker_host[HOST_CHIRPSTACK_DEST]["ssl_mode"]
                client_id = f'{client_id}_{DEVICE_BRAND}_{MQTTHostChoice.CHRIPSTACK_HOST_1.value}'
            case MQTTHostChoice.CHRIPSTACK_HOST_2.value:
                broker = broker_host[HOST_CHIRPSTACK_DEST]["host"]
                broker_port = broker_host[HOST_CHIRPSTACK_DEST]["port"]
                ssl_mode = broker_host[HOST_CHIRPSTACK_DEST]["ssl_mode"]
                client_id = f'{client_id}_{DEVICE_BRAND}_{MQTTHostChoice.CHRIPSTACK_HOST_2.value}'
    case _:
        broker = broker_host[MQTTHostChoice.HOST_IOT_CORE.value]["host"]
        broker_port = broker_host[MQTTHostChoice.HOST_IOT_CORE.value]["port"]
        ssl_mode = broker_host[MQTTHostChoice.HOST_IOT_CORE.value]["ssl_mode"]
        client_id = f'{client_id}_{DEVICE_BRAND}'

mqtt_config = {
    "MQTT_CLIENT_ID": client_id,
    "MQTT_BROKER_URL": broker,
    "MQTT_BROKER_PORT": broker_port,
    "MQTT_USERNAME": '',
    "MQTT_PASSWORD": '',
    "MQTT_KEEPALIVE": 120,
    "MQTT_TLS_ENABLED": ssl_mode,
    "MQTT_TLS_CA_CERTS": CA_PATH,
    "MQTT_TLS_CERTFILE": CERT_PATH,
    "MQTT_TLS_KEYFILE": KEY_PATH,
    "MQTT_TLS_VERSION": ssl.PROTOCOL_TLSv1_2,
    "MQTT_TLS_CERT_REQS": ssl.CERT_REQUIRED,
    "MQTT_TLS_CIPHERS": None
}
sqlalchemy_config = {
    "SQLALCHEMY_DATABASE_URI": f"postgresql://{DATABASES_USER}:{DATABASES_PASSWORD}@{DATABASES_HOST}:{DATABASES_PORT}/{DATABASES_NAME}"
}


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_mapping({**mqtt_config, **sqlalchemy_config})
    db.init_app(app)
    with app.app_context():
        initial_query_device()
    api.init_app(app)
    mqtt_client.init_app(app)
    
    return app