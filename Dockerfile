# pull official base image
FROM python:3.9
WORKDIR /app

RUN apt-get update 

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt


COPY . .
