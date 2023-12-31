FROM python:3.10.8

ENV PYTHONUNBUFFERED 1

WORKDIR /code

RUN pip install --upgrade pip
COPY /requirements.txt /code/
RUN pip install -r requirements.txt
COPY . /code/

CMD ["python", "./main.py"]