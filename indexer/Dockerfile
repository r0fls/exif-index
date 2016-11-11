FROM debian:jessie
RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get -y install python2.7 python-pip
ADD . /home/app
RUN pip install -r /home/app/requirements.txt
ENTRYPOINT ["./app.py"]
WORKDIR /home/app
