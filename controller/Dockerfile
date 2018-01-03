FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y python3-dev python3-pip git

COPY requirements.txt /opt/requirements.txt
RUN pip3 install -r /opt/requirements.txt

COPY controller.py /opt/controller.py
RUN chmod +x /opt/controller.py

ENTRYPOINT ["python3", "/opt/controller.py"]

