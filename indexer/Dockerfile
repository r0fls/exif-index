FROM debian:jessie
RUN apt-get update
RUN apt-get -y upgrade
#RUN apt-get -y install python2.7 python-pip
RUN apt-get -y install python3 python3-pip
ADD . /home/app
RUN pip3 install -r /home/app/requirements.txt
WORKDIR /home/app
ENTRYPOINT ["./app.py"]
