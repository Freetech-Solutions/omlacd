FROM omnileads/asterisk_base_img:250222.01 as run

ENV LANG en_US.utf8
ENV NOTVISIBLE "in users profile"

COPY build/requirements.txt /

RUN echo "deb http://ftp.de.debian.org/debian bullseye main non-free" >> /etc/apt/sources.list
RUN apt update && apt install -y --no-install-recommends \
    sngrep curl \
    && pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r /requirements.txt \
    && apt autoremove -y && apt clean -y && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY source/astconf/* /etc/asterisk/
COPY source/astconf/retrieve_conf/ /etc/asterisk/retrieve_conf/
COPY source/scripts/* /opt/asterisk/scripts/
COPY source/agi-bin/* /var/lib/asterisk/agi-bin/
COPY build/docker-entrypoint.sh /docker-entrypoint.sh

RUN chmod 750 -R /var/spool/asterisk && \
    useradd -M omnileads && \
    chown -R omnileads:omnileads /var/lib/asterisk /etc/asterisk /opt/asterisk /usr/lib/asterisk /docker-entrypoint.sh /var/spool/asterisk /var/log/asterisk

EXPOSE 5060/udp

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD [""]
