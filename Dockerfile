FROM python:3.10.12-slim-bullseye as dev

ENV LANG en_US.utf8
ENV NOTVISIBLE "in users profile"

RUN pip install --upgrade pip

COPY build/build-asterisk.sh .asterisk_version build/requirements.txt /
RUN ASTERISK_VERSION=$(cat .asterisk_version) /build-asterisk.sh
RUN apt update \
    && apt install -y libgsm1 git curl python3-psycopg2 gnupg \
    && pip install -r /requirements.txt \
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

FROM python:3.10.12-slim-bullseye as run

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

RUN cp -a /usr/local/lib/python3.10/site-packages/pyst2 /src/ 

COPY source/astconf/* /etc/asterisk/
COPY source/scripts/* /opt/asterisk/scripts/
COPY build/docker-entrypoint.sh /docker-entrypoint.sh

RUN mkdir /etc/asterisk/custom
RUN chmod 750 /etc/asterisk/custom /var/spool/asterisk
RUN useradd -M -u 1000 omnileads
RUN chown -R omnileads.omnileads /var/lib/asterisk /etc/asterisk /opt/asterisk /usr/lib/asterisk /docker-entrypoint.sh /var/spool/asterisk

EXPOSE 5060/udp

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD [""]
