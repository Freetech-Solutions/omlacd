# vim:set ft=dockerfile:
FROM freetechsolutions/omlacd-builder:latest as builder

FROM python:3.7-slim as production

RUN mkdir /src \
    && apt-get update -qq \
    && apt-get install -y libbinutils libedit2 libncursesw5 \
    && apt autoremove -y

# Todos los paquetes de asterisk
COPY --from=builder /usr/lib/libasteris* /usr/lib/
COPY --from=builder /etc/asterisk /etc/asterisk/
COPY --from=builder /var/lib/asterisk /var/lib/asterisk
COPY --from=builder /var/log/asterisk /var/log/asterisk
COPY --from=builder /var/spool/asterisk /var/spool/asterisk
COPY --from=builder /root/bin/* /usr/sbin/
COPY --from=builder /usr/lib/asterisk /usr/lib/asterisk/
COPY --from=builder /var/run/asterisk/ /var/run/asterisk/

# Librerias de python, curl otras librerias necesarias
COPY --from=builder /usr/local/lib/python3.7/ /usr/local/lib/python3.7/
COPY --from=builder /usr/lib/x86_64-linux-gnu/ /usr/lib/x86_64-linux-gnu/

RUN cp -a /usr/local/lib/python3.7/site-packages/pyst2 /src/ && \
  mkdir -p /opt/omnileads/asterisk/virtualenv/bin/ && \
  ln -s /usr/local/bin/python3 /opt/omnileads/asterisk/virtualenv/bin/

COPY source/astconf/* /etc/asterisk/
COPY source/agis/* /var/lib/asterisk/agi-bin/
COPY source/odbc/*.ini /etc/
COPY source/sounds/* /var/lib/asterisk/sounds/
COPY source/scripts/ /opt/omnileads/asterisk/virtualenv/scripts/
COPY build/docker/docker-entrypoint.sh /docker-entrypoint.sh

EXPOSE 5038/tcp 7088/tcp 5160-5163/udp 5060/udp

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD [""]
