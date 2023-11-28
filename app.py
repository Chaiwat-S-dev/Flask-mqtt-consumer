import ssl
from flask import Flask
from model.models import db
from mqtt.mqtt import mqtt_client, initial_query_device
from utils.cache import cache
from route import api
from utils.config import *


mqtt_config = {
    "MQTT_BROKER_URL": AWS_HOST,
    "MQTT_BROKER_PORT": AWS_PORT,
    "MQTT_USERNAME": '',
    "MQTT_PASSWORD": '',
    "MQTT_KEEPALIVE": 120,
    "MQTT_TLS_ENABLED": SSL_MODE,
    "MQTT_TLS_CA_CERTS": CA_PATH,
    "MQTT_TLS_CERTFILE": CERT_PATH,
    "MQTT_TLS_KEYFILE": KEY_PATH,
    "MQTT_TLS_VERSION": ssl.PROTOCOL_TLSv1_2,
    "MQTT_TLS_CERT_REQS": ssl.CERT_REQUIRED,
    "MQTT_TLS_CIPHERS": None
}
redis_config = {
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300
}
sqlalchemy_config = {
    "SQLALCHEMY_DATABASE_URI": "postgresql://postgres:1234567890@localhost:5432/iot_dev"
}

def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_mapping({**mqtt_config, **redis_config, **sqlalchemy_config})
    db.init_app(app)
    with app.app_context():
        initial_query_device()
    cache.init_app(app)
    api.init_app(app)
    mqtt_client.init_app(app)
    
    return app