FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y docker.io

RUN apt-get install -y python3-dev python3-pip gcc libsqlite3-dev libssl-dev libffi-dev
RUN pip3 install jsonschema simplejson boto3 awscli boutiques==0.5.5-dev1 reprozip
RUN reprozip usage_report --disable

RUN mkdir /clowtask /clowdata &&\
		chmod 777 /clowtask /clowdata
COPY entrypoint.py /entrypoint.py
RUN chmod +x /entrypoint.py

ENTRYPOINT ["python3", "/entrypoint.py"]
