FROM ubuntu:18.04

RUN apt update \
    && apt install -y python3-pip

COPY . src/
RUN /bin/bash -c "cd src \
    && pip3 install pandas==1.1.5"