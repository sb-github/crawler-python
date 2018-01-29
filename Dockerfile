FROM ubuntu:16.04

ENV PYTHONIOENCODING=utf-8
USER root

WORKDIR /crawler_app

RUN apt-get update && apt-get -y install software-properties-common && add-apt-repository ppa:jonathonf/python-3.6
RUN apt-get update && apt-get -y install python3.6 && apt-get -y install python3-pip && apt-get -y install git

RUN git clone -b develop https://github.com/sb-github/crawler-python.git
# COPY ./crawler-python /crawler_app

# RUN pip3 install -r requirements.txt
RUN pip3 install -r crawler-python/requirements.txt

EXPOSE 5000

# CMD cd crawler-python/rest_api; /usr/bin/python3 app.py
CMD cd crawler-python/cron_job; /usr/bin/python3 cron_schedule.py; cd ../rest_api; /usr/bin/python3 app.py

# sudo docker build --no-cache -t crawler-python:latest .
# sudo docker run -it --rm -e LANG=C.UTF-8 -p 5000:5000 crawler-python
