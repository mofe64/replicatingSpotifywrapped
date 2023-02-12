FROM python:3.9


RUN apt-get install wget
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data  data



WORKDIR /app
