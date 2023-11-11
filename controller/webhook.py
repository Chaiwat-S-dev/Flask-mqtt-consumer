from flask_restful import Resource
from mqtt.mqtt import mqtt_client, device_topic_struct, initial_query_device

class Webhook(Resource):

    def post(self):
        initial_query_device()
        mqtt_client.unsubscribe_all()

        for topic in device_topic_struct.topic_listen:
            mqtt_client.subscribe(topic)

        return {"message": 'success'}, 200