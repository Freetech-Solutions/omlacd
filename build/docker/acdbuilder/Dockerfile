FROM python:3.7-slim as builder

ENV LANG en_US.utf8
ENV NOTVISIBLE "in users profile"
ENV OPUS_CODEC asterisk-16.0/x86-64/codec_opus-16.0_current-x86_64

COPY build-asterisk.sh .asterisk_version requirements.txt /
RUN ASTERISK_VERSION=$(cat .asterisk_version) /build-asterisk.sh
RUN apt-get install -y odbc-postgresql libgsm1 git curl python3-psycopg2 gnupg \
    && pip3 install -r /requirements.txt \
    && echo "deb http://packages.irontec.com/debian stretch main" >> /etc/apt/sources.list \
    && wget http://packages.irontec.com/public.key -q -O - | apt-key add - \
    && apt-get update -y \
    && apt-get install sngrep -y \
    && apt-get remove --purge git -y \
    && apt autoremove -y \
    && mkdir /root/bin/  \
    && cp /usr/sbin/ast* /usr/bin/curl /usr/bin/sngrep /root/bin \
    && cp -a /src/pyst2/ /usr/local/lib/python3.7/site-packages/ \
    && cp -a /usr/lib/python3/dist-packages/psycopg2/ /usr/local/lib/python3.7/site-packages/ \
    && cp -a /lib/x86_64-linux-gnu/libkeyutils.so.1* /usr/lib/x86_64-linux-gnu/ \
    && rm -rf /usr/src/asterisk \
    && rm -rf /usr/include/asterisk \
    && apt autoremove -y
