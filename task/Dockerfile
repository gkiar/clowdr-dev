FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y docker.io

RUN apt-get install -y python3-dev python3-pip
RUN pip3 install jsonschema simplejson boto3 awscli boutiques

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
RUN mkdir /task /data

ENTRYPOINT ["/app/entrypoint.sh"]
