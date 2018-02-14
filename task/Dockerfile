FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y docker.io

RUN apt-get install -y python3-dev python3-pip gcc libsqlite3-dev libssl-dev libffi-dev
RUN apt-get install -y git
RUN pip3 install jsonschema simplejson boto3 awscli reprozip
RUN git clone https://github.com/gkiar/boutiques.git /opt/boutiques/ &&\
    cd /opt/boutiques/tools/python &&\
    git checkout gk-dev &&\
    pip3 install -e .
RUN reprozip usage_report --disable

RUN mkdir /clowtask /clowdata &&\
		chmod 777 /clowtask /clowdata
COPY entrypoint.py /entrypoint.py
RUN chmod +x /entrypoint.py

ENTRYPOINT ["python3", "/entrypoint.py"]
