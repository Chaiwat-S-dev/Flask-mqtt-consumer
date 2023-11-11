import os
import logging
from utils.config import CURRENT_DIR

class SingletonLogger:
    _logger_instance = None
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        log_dir = os.path.join(CURRENT_DIR, 'middleware_logs')
        if not os.path.isdir(log_dir):
            os.mkdir(log_dir)
        
        file_handler = logging.FileHandler(f'{log_dir}/error.log', mode='a', delay=False)
        file_handler.setLevel(logging.ERROR)

        format_log = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')

        console_handler.setFormatter(format_log)
        file_handler.setFormatter(format_log)
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
    
    @classmethod
    def get_logger_instance(cls):
        if not cls._logger_instance:
            cls._logger_instance = cls()
        return cls._logger_instance