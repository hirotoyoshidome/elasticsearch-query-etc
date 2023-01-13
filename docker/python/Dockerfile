FROM python:3.9.16-slim-bullseye

WORKDIR /opt

RUN apt update && pip install --upgrade pip

COPY requirements.txt .
RUN pip install -r requirements.txt
