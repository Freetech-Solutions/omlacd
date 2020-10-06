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
COPY --from=builder /root/ast-db-manage/ /root/ast-db-manage/

# Librerias de python, curl otras librerias necesarias
COPY --from=builder /usr/local/lib/python3.7/ /usr/local/lib/python3.7/
COPY --from=builder /usr/lib/x86_64-linux-gnu/ /usr/lib/x86_64-linux-gnu/

RUN cp -a /usr/local/lib/python3.7/site-packages/pyst2 /src/

COPY asterisk/conf/* /etc/asterisk/
COPY asterisk/agi-bin/* /var/lib/asterisk/agi-bin/
COPY asterisk/etc/*.ini /etc/
COPY asterisk/sounds/* /var/lib/asterisk/sounds/
COPY scripts/docker-entrypoint.sh /docker-entrypoint.sh

EXPOSE 5038/tcp 7088/tcp 5160-5163/udp 5060/udp

ENTRYPOINT ["/docker-entrypoint.sh"]
