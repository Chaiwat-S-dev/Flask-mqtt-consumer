from app import create_app
from utils.config import PORT, DEBUG

if __name__ == '__main__':
   app = create_app()
   app.run(host='0.0.0.0', port=PORT, debug=DEBUG)
   