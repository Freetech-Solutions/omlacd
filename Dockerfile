# vim:set ft=dockerfile:
FROM debian:buster-slim

ENV LANG en_US.utf8
ENV NOTVISIBLE "in users profile"
ENV ASTERISK_VERSION 16.12.0
ENV OPUS_CODEC asterisk-16.0/x86-64/codec_opus-16.0_current-x86_64

COPY scripts/build-asterisk.sh /
RUN /build-asterisk.sh

RUN apt-get install -y git iproute2 net-tools python3-minimal python3-psycopg2 bash odbc-postgresql less python3-pip wget gnupg lame postgresql-client-11 \
    && echo "deb http://packages.irontec.com/debian stretch main" >> /etc/apt/sources.list \
    && wget http://packages.irontec.com/public.key -q -O - | apt-key add - \
    && apt-get update -y \
    && apt-get install sngrep libgsm1 -y \
    && pip3 install -e git+https://github.com/rdegges/pyst2@master#egg=pyst2 \
    && pip3 install 'six==1.10.0' 'redis==3.5.3' alembic \
    && apt-get remove -y --purge python3-pip git \
    && apt autoremove -y \
    && apt-get install -y libedit2 libbinutils

COPY asterisk/dialplan/* /etc/asterisk/
COPY asterisk/conf/* /etc/asterisk/
COPY asterisk/agi-bin/* /var/lib/asterisk/agi-bin/
COPY asterisk/etc/*.ini /etc/
COPY asterisk/sounds/* /var/lib/asterisk/sounds/
COPY scripts/run_asterisk.sh /home/

EXPOSE 22 5038/tcp 7088/tcp 5060/udp 5060/tcp
