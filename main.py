from app import create_app
from mqtt.mqtt import initial_query_device
from utils.config import PORT, DEBUG

if __name__ == '__main__':
   app = create_app()
   with app.app_context():
        initial_query_device()
   app.run(host='0.0.0.0', port=PORT, debug=DEBUG)
   