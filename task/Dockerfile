FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y docker.io

RUN apt-get install -y python3-dev python3-pip
RUN pip3 install jsonschema simplejson boto3 awscli boutiques

COPY entrypoint.py /opt/entrypoint.py
RUN chmod +x /opt/entrypoint.py &&\
		mkdir /task /data &&\
		chmod 777 /task /data

ENTRYPOINT ["/opt/entrypoint.py"]
