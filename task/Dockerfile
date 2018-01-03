FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y docker.io

RUN apt-get install -y python3-dev python3-pip
RUN pip3 install jsonschema simplejson boto3 awscli boutiques==0.5.5-dev1

RUN mkdir /clowtask /clowdata &&\
		chmod 777 /clowtask /clowdata
COPY entrypoint.py /opt/entrypoint.py
RUN chmod +x /opt/entrypoint.py

ENTRYPOINT ["python3", "/opt/entrypoint.py"]
