FROM python:3.10.8-slim

ENV PYTHONUNBUFFERED 1

WORKDIR /code

RUN apt update && apt install -y cron
RUN pip install --upgrade pip

COPY /requirements.txt /code/

RUN python -m pip install --no-cache-dir --upgrade pip 
RUN pip install --no-cache-dir -r requirements.txt

COPY . /code/

CMD ["bash", "-c", "printenv > /etc/default/locale && /etc/init.d/cron start && python main.py"]

EXPOSE 5000