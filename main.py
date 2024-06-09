from app import create_app
from utils.config import HOST, PORT, DEBUG, CURRENT_DIR
from crontab import CronTab

if __name__ == '__main__':
   cron = CronTab(user=True)
   job = cron.new(command=f'/usr/local/bin/python {CURRENT_DIR}/receiver.py > /var/log/receiver_$(date +"%d-%m-%Y").txt 2>&1')
   job.setall('0 0 * * *')
   # cron.write()
   app = create_app()
   app.run(host=HOST, port=PORT, debug=DEBUG)