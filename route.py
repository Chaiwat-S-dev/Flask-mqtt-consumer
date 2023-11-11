from flask_restful import Api
from controller.device import DeviceList, Device
from controller.webhook import Webhook

api = Api()

api.add_resource(Device, '/devices/<int:id>')
api.add_resource(DeviceList, '/devices')
api.add_resource(Webhook, '/webhook')