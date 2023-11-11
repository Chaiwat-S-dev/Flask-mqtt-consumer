from flask_restful import Resource, reqparse, abort
from model.models import db, DeviceModel
import json
from utils.cache import cache

class DeviceList(Resource):
    def get(self):
        if not (json_context := cache.get("device_list")):
            list_devices = db.session.query(DeviceModel).order_by(DeviceModel.id.desc()).all()
            context = [device.to_dict for device in list_devices]
            json_context = json.dumps(context)
            cache.set("device_list", json_context)
        
        return json.loads(json_context), 200

class Device(Resource):
    def get(self, id):
        device = db.get_or_404(DeviceModel, id=id)
        return device.to_dict, 200