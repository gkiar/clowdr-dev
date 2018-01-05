FROM jpetazzo/dind:latest

RUN apt-get update
RUN apt-get install -y automake \
                       autotools-dev \
                       fuse \
                       g++ \
                       git \
                       libcurl4-gnutls-dev \
                       libfuse-dev \
                       libssl-dev \
                       libxml2-dev \
                       make \
                       pkg-config

RUN apt-get install -y python3-dev \
                       python3-pip

RUN git clone https://github.com/s3fs-fuse/s3fs-fuse.git /opt/s3fs-fuse && \
    cd /opt/s3fs-fuse && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make install

RUN pip3 install jsonschema \
                 simplejson \
                 boto3 \
                 awscli \
                 boutiques==0.5.5-dev1

RUN mkdir /clowtask /clowdata && \
		chmod 777 /clowtask /clowdata

COPY task.py /opt/task.py
RUN chmod +x /opt/task.py

COPY .bashrc ~/.bashrc

ENTRYPOINT ["wrapdocker"]
