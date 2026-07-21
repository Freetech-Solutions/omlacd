FROM --platform=linux/arm64 docker.io/freetechsolutions/asterisk:20260707-91edd45e AS run

ENV LANG=en_US.utf8
ENV NOTVISIBLE="in users profile"

COPY build/requirements.txt /

# sngrep: debug SIP. git solo para pip VCS (gearman3); se purga al final
# para no dejar Perl/CVEs de git en la imagen runtime.
RUN apt update && apt install -y --no-install-recommends \
    sngrep \
    git \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt \
    && pip install --no-cache-dir --upgrade 'wheel>=0.46.2' 'jaraco.context>=6.1.0' \
    && apt-get purge -y --auto-remove git \
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

# Certs TLS: no bakear key.pem en la imagen (Trivy secret). Se generan en entrypoint
# si faltan, o se montan en runtime (prod).
RUN mkdir -p /etc/asterisk/certs && \
    chmod 750 -R /var/spool/asterisk && \
    useradd -M omnileads && \
    chown -R omnileads:omnileads /var/lib/asterisk /etc/asterisk /opt/asterisk /usr/lib/asterisk /docker-entrypoint.sh /var/spool/asterisk /var/log/asterisk /opt/asterisk/workers
