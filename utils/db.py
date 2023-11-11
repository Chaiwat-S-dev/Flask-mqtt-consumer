import psycopg2
from utils.config import DATABASES_NAME, DATABASES_USER, DATABASES_PASSWORD, \
    DATABASES_HOST, DATABASES_PORT, DEBUG

from .logging import SingletonLogger

log = SingletonLogger.get_logger_instance().logger

class DB:
    client, cur = None, None
    config = {}
    device_db = {}

    def __init__(self):
        r"""
        :param username: Username of Database
        :param password: Password of Database
        :param db_name: Database Name
        :param host: Database Host (IP or URL)
        :param port: Database Port Default 5432
        """
        self.config = {
            "dbname": DATABASES_NAME,
            "user": DATABASES_USER,
            "password": DATABASES_PASSWORD,
            "host": DATABASES_HOST,
            "port": DATABASES_PORT
        }

    def _connect(self):
        try:
            self.client = psycopg2.connect(**self.config)
            self.cur = self.client.cursor()
            log.info("PostgreSQL server information")
            log.info(self.client.get_dsn_parameters())

            return self.cur

        except (Exception, psycopg2.Error) as error:
            log.error(error)
            raise error

    def query(self):
        '''
        Example device paload
        # self.device_db = {
        #     '0018051F8CE3': {
        #         'bucket': 'demo', 
        #         'measurement': 'modbus', 
        #         'organization': 'swift_dynamics',
        #         'parameters': {
        #             "flow_main2":{
        #                 "mb001": ["raw_data", "timestamp", "status"]
        #             },
        #             "flow_main1":{
        #                 "mb001": ["raw_data", "timestamp", "status"]
        #             },
        #             "Modbus-TCP-LOGO":{
        #                 "Coil-Write":["raw_data", "timestamp", "status"],
        #                 "Coil-INPUT1":["raw_data", "timestamp", "status"],
        #                 "Coil-INPUT2":["raw_data", "timestamp", "status"],
        #             },
        #             "MB-RTU1":{
        #                 "Alarm":["raw_data", "timestamp", "status"],
        #                 "Energy_High":["raw_data", "timestamp", "status"],
        #                 "Energy_Low":["raw_data", "timestamp", "status"],
        #                 "Power_High":["raw_data", "timestamp", "status"],
        #                 "Power_Low":["raw_data", "timestamp", "status"],
        #                 "PowerFactor":["raw_data", "timestamp", "status"],
        #                 "Frequency":["raw_data", "timestamp", "status"],
        #                 "Current_High":["raw_data", "timestamp", "status"],
        #                 "Current_Low":["raw_data", "timestamp", "status"],
        #                 "Voltage":["raw_data", "timestamp", "status"],
        #             },
        #             "MB-RTU2":{
        #                 "Energy_High":["raw_data", "timestamp", "status"],
        #                 "Energy_Low":["raw_data", "timestamp", "status"],
        #                 "Power_High":["raw_data", "timestamp", "status"],
        #                 "Power_Low":["raw_data", "timestamp", "status"],
        #                 "PowerFactor":["raw_data", "timestamp", "status"],
        #                 "Frequency":["raw_data", "timestamp", "status"],
        #                 "Current_High":["raw_data", "timestamp", "status"],
        #                 "Current_Low":["raw_data", "timestamp", "status"],
        #                 "Voltage":["raw_data", "timestamp", "status"],
        #             }
        #         },
        #         'brand': 'inhand'
        #     }
        # }
        '''

        cur = self._connect()
        query_statement = '''
            SELECT  device.mac_address, device.bucket, device.measurement, 
                    org.code AS org_code, device.brand, device.parameters,
                    device.app_id
            FROM public.apis_device AS device 
            JOIN public.apis_organization AS org ON device.organization_id = org.id 
            WHERE device.is_activated = TRUE AND device.brand NOT LIKE 'chirpstack'  
        '''

        cur.execute(query_statement)

        rows = cur.fetchall()
        for row in rows:
            self.device_db.update({
                row[0]: {
                    "bucket": row[1],
                    "measurement": row[2],
                    "organization": row[3],
                    "brand": row[4],
                    "parameters": row[5],
                    "app_id": row[6],
                }
            })

        if DEBUG:
            log.info(f'[DB]: {self.device_db=}')

        cur.close()
        self.client.close()

        return self.device_db
