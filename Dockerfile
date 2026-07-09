FROM --platform=linux/arm64 docker.io/freetechsolutions/asterisk:20260707-91edd45e AS run

ENV LANG=en_US.utf8
ENV NOTVISIBLE="in users profile"

COPY build/requirements.txt /

# RUN echo "deb http://ftp.de.debian.org/debian trixie main non-free" >> /etc/apt/sources.list
RUN apt update && apt install -y --no-install-recommends \
    sngrep git \
    && pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r /requirements.txt \
    && apt autoremove -y && apt clean -y && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV PYTHONPATH=/opt/asterisk/ari-app

COPY source/astconf/ /etc/asterisk/
COPY source/astconf/retrieve_conf/ /etc/asterisk/retrieve_conf/
COPY source/ari-app/ /opt/asterisk/ari-app/
COPY source/tests_unit/ /opt/asterisk/source/tests_unit/
COPY source/workers/ /opt/asterisk/workers/
COPY .flake8 /opt/asterisk/.flake8
COPY pytest.ini /opt/asterisk/pytest.ini
COPY build/docker-entrypoint.sh /docker-entrypoint.sh
COPY certs/README.md /etc/asterisk/certs/README.md

RUN mkdir -p /etc/asterisk/certs && \
    openssl req -x509 -newkey rsa:4096 \
      -keyout /etc/asterisk/certs/key.pem \
      -out /etc/asterisk/certs/cert.pem \
      -days 3650 -nodes -subj "/CN=acd-dev.local" && \
    chmod 750 -R /var/spool/asterisk && \
    useradd -M omnileads && \
    chown -R omnileads:omnileads /var/lib/asterisk /etc/asterisk /opt/asterisk /usr/lib/asterisk /docker-entrypoint.sh /var/spool/asterisk /var/log/asterisk /opt/asterisk/workers
