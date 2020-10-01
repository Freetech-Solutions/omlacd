# vim:set ft=dockerfile:
FROM freetechsolutions/omlacd-builder:latest as builder

FROM python:3.7-slim as production

RUN apt-get update -qq \
    && apt-get install -y libbinutils libedit2 \
    && apt autoremove -y

# Todos los paquetes de asterisk
COPY --from=builder /usr/lib/libasteriskssl.so.1 /usr/lib/libasteriskssl.so.1
COPY --from=builder /usr/lib/libasteriskpj.so.2 /usr/lib/libasteriskpj.so.2
COPY --from=builder /etc/asterisk /etc/asterisk/
COPY --from=builder /var/lib/asterisk /var/lib/asterisk
COPY --from=builder /var/log/asterisk /var/log/asterisk
COPY --from=builder /var/spool/asterisk /var/spool/asterisk
COPY --from=builder /usr/sbin/asterisk /usr/sbin/
COPY --from=builder /usr/lib/asterisk /usr/lib/asterisk/
COPY --from=builder /var/run/asterisk/ /var/run/asterisk/
COPY --from=builder /root/ast-db-manage/ /root/ast-db-manage/

# Librerias de python, curl otras librerias necesarias
COPY --from=builder /usr/local/lib/python3.7/ /usr/local/lib/python3.7/
COPY --from=builder /usr/local/bin/alembic /usr/local/bin/alembic
COPY --from=builder /usr/bin/curl /usr/bin/curl
COPY --from=builder /usr/lib/x86_64-linux-gnu/ /usr/lib/x86_64-linux-gnu/

COPY asterisk/conf/* /etc/asterisk/
COPY asterisk/agi-bin/* /var/lib/asterisk/agi-bin/
COPY asterisk/etc/*.ini /etc/
COPY asterisk/sounds/* /var/lib/asterisk/sounds/
COPY scripts/run_asterisk.sh /home/

EXPOSE 22 5038/tcp 7088/tcp 5060/udp 5060/tcp
