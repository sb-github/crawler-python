FROM debian:latest

WORKDIR /crawler_app

# Update debian and install python
RUN sudo apt-get update -y && sudo apt-get install -y python3.6
# Install pip and all the requirements
RUN sudo apt-get install -y python3-pip && sudo pip3 install -r requirements.txt
# Install git
RUN sudo apt-get install git-core

EXPOSE 5000

COPY . /crawler_app

# ADD . /crawler_app

# CMD ["python3", "cron_schedule.py"]
CMD ["export", "FLASK_APP=app.py"]
CMD ["flask", "run"]