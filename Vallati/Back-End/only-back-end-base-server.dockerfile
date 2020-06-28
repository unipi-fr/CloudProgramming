FROM python:3-alpine
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
# Install the PIKA library
RUN pip3 install pika
RUN pip3 install kazoo

RUN apk add --update --no-cache mariadb-connector-c-dev \
    && apk add --no-cache --virtual .build-deps \
    mariadb-dev \
    gcc \ 
    musl-dev \ 
    && pip install mysqlclient==1.4.6 \ 
    && apk del .build-deps

ENV PYTHONUNBUFFERED=1