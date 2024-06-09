from enum import Enum

class MQTTHostChoice(Enum):
    CHRIPSTACK_HOST_1 = 1
    CHRIPSTACK_HOST_2 = 2
    HOST_IOT_CORE = 3

broker_host = {
    1: {
        "host": "localhost",
        "port": 1883,
        "ssl_mode": False,
        "help": "chirpstack host1"
    },
    2: {
        "host": "localhost",
        "port": 1883,
        "ssl_mode": False,
        "help": "chirpstack host2"
    },
    3: {
        "host": "a31dvipl38h049-ats.iot.ap-southeast-1.amazonaws.com",
        "port": 8883,
        "ssl_mode": True,
    },
}