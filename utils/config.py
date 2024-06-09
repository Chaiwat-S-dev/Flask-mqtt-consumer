import os

CURRENT_DIR = os.getcwd()
HOST = os.environ.get("HOST", '0.0.0.0')
PORT = os.environ.get("PORT", 5000)

INFLUX_URL = os.environ.get("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN",
                        "Vd3yJYrdCWiTdNqa0iUe24Ou-lGecauOxbiD7hH3SPWPsSxRZx5On0rNS5yw1J1Xi0NkeQDa4F6wx7X84wrjMw==")

FRASER_INFLUX_URL=os.environ.get("FRASER_INFLUX_URL", "http://localhost:8086")
FRASER_INFLUX_TOKEN=os.environ.get("FRASER_INFLUX_TOKEN",
                        "Vd3yJYrdCWiTdNqa0iUe24Ou-lGecauOxbiD7hH3SPWPsSxRZx5On0rNS5yw1J1Xi0NkeQDa4F6wx7X84wrjMw==")

MQTT_CLIENT_ID = 'mqtt_consumer'

SSL_MODE = bool(os.environ.get("SSL_MODE", True))
MQTT_HOST = os.environ.get("MQTT_BROKER", "192.168.11.29")
MQTT_PORT = int(os.environ.get("BROKER_PORT", 1883))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "")

CA_PATH = os.path.join(CURRENT_DIR, "cert", os.environ.get("CA_PATH", "AmazonRootCA1.pem"))
CERT_PATH = os.path.join(CURRENT_DIR, "cert", os.environ.get("CERT_PATH", "iot-core-certificate.pem.crt"))
KEY_PATH = os.path.join(CURRENT_DIR, "cert", os.environ.get("KEY_PATH", "iot-core-private.pem.key"))

DATABASES_NAME = os.environ.get("DATABASES_NAME", "iot_dev")
DATABASES_USER = os.environ.get("DATABASES_USER", "postgres")
DATABASES_PASSWORD = os.environ.get("DATABASES_PASSWORD", "1234567890")
DATABASES_HOST = os.environ.get("DATABASES_HOST", "localhost")
DATABASES_PORT = os.environ.get("DATABASES_PORT", "5432")

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
QUEUENAME = os.environ.get("QUEUENAME", "consumer_service")

DEVICE_BRAND = os.environ.get("DEVICE_BRAND", "chirpstack")
HOST_CHIRPSTACK_DEST = int(os.environ.get("HOST_CHIRPSTACK_DEST", "1"))

DEBUG = os.environ.get("DEBUG", "True") == "True"