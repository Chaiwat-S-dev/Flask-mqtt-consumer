bind = "0.0.0.0:5000"
workers = 1
timeout = 120
loglevel = "error"
accesslog = "logs/gunicorn_accesslog.log"
errorlog = "logs/gunicorn_error.log"
capture_output = True