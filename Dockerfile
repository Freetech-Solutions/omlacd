# vim:set ft=dockerfile:


FROM python:3.10.4-slim-bullseye as dev

ENV LANG en_US.utf8
ENV NOTVISIBLE "in users profile"

COPY build//build-asterisk.sh .asterisk_version_deb build//requirements.txt /
RUN ASTERISK_VERSION=$(cat /.asterisk_version_deb) /build-asterisk.sh
RUN apt update \
    && apt install -y libgsm1 git curl python3-psycopg2 gnupg \
    && pip3 install -r /requirements.txt \
    && echo "deb http://packages.irontec.com/debian stretch main" >> /etc/apt/sources.list \
    && wget http://packages.irontec.com/public.key -q -O - | apt-key add - \
    && apt-get update -y \
    && apt install sngrep -y \
    && apt-get remove --purge git -y \
    && apt autoremove -y \
    && mkdir /root/bin/  \
    && cp /usr/sbin/ast* /usr/bin/curl /usr/bin/sngrep /root/bin \
    && cp -a /src/pyst2/ /usr/local/lib/python3.10/site-packages/ \
    && cp -a /usr/lib/python3/dist-packages/psycopg2/ /usr/local/lib/python3.10/site-packages/ \
    && cp -a /lib/x86_64-linux-gnu/libkeyutils.so.1* /usr/lib/x86_64-linux-gnu/ \
    && rm -rf /usr/src/asterisk \
    && rm -rf  /usr/sbin/ast* \
    && rm -rf /usr/include/asterisk \
    && rm -rf /src/pyst2/ \
    && rm -rf /usr/lib/python3/dist-packages/psycopg2/ \
    && rm -rf /lib/x86_64-linux-gnu/libkeyutils.so.1*

FROM python:3.10.4-slim-bullseye as run

RUN mkdir /src \
    && apt-get update -qq \
    && apt install -y libbinutils libedit2 libncursesw5 wget awscli \
    && apt autoremove -y \
    && apt clean -y \
    && apt purge -y \
    && rm -rf /var/lib/apt/lists/*

# Todos los paquetes de asterisk
COPY --from=dev /usr/lib/libasteris* /usr/lib/
COPY --from=dev /etc/asterisk /etc/asterisk/
COPY --from=dev /var/lib/asterisk /var/lib/asterisk
COPY --from=dev /var/log/asterisk /var/log/asterisk
COPY --from=dev /var/spool/asterisk /var/spool/asterisk
COPY --from=dev /root/bin/* /usr/sbin/
COPY --from=dev /usr/lib/asterisk /usr/lib/asterisk/
COPY --from=dev /var/run/asterisk/ /var/run/asterisk/

# Librerias de python, curl otras librerias necesarias
COPY --from=dev /usr/local/lib/python3.10/ /usr/local/lib/python3.10/
COPY --from=dev /usr/lib/x86_64-linux-gnu/ /usr/lib/x86_64-linux-gnu/

RUN cp -a /usr/local/lib/python3.10/site-packages/pyst2 /src/ && \
  mkdir -p /opt/asterisk/virtualenv/bin/ && \
  ln -s /usr/local/bin/python3 /opt/asterisk/virtualenv/bin/

COPY source/astconf/* /etc/asterisk/
COPY source/agis/* /var/lib/asterisk/agi-bin/
COPY source/odbc/*.ini /etc/
COPY source/scripts/* /opt/asterisk/virtualenv/scripts/
COPY build//docker-entrypoint.sh /docker-entrypoint.sh

RUN useradd -M -u 1000 asterisk

EXPOSE 5060/udp 5160/udp 40000-50000/udp

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD [""]
