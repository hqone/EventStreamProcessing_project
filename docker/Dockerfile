FROM python:3

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y default-jdk
RUN pip install kafka-python pyspark
COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
