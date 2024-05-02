FROM python:3.12-slim-bullseye as dev

# Configuración de entorno
ENV LANG en_US.utf8
ENV NOTVISIBLE "in users profile"

# Actualización de pip y copia de scripts necesarios
RUN pip install --upgrade pip

COPY build/build-asterisk.sh .asterisk_version build/requirements.txt /
RUN ASTERISK_VERSION=$(cat .asterisk_version) /build-asterisk.sh
RUN echo "deb http://ftp.de.debian.org/debian bullseye main non-free" >> /etc/apt/sources.list
RUN apt update \
    && apt install -y libgsm1 git curl gnupg libttspico-utils sox \
    && pip install -r /requirements.txt \
    && apt-get update -y \
    && apt install sngrep -y \
    && apt-get remove --purge git -y \
    && apt autoremove -y \
    && mkdir /root/bin/  \
    && cp /usr/sbin/ast* /usr/bin/curl /usr/bin/sngrep /root/bin \    
    && cp -a /lib/x86_64-linux-gnu/libkeyutils.so.1* /usr/lib/x86_64-linux-gnu/ \
    && rm -rf /usr/src/asterisk \
    && rm -rf  /usr/sbin/ast* \
    && rm -rf /usr/include/asterisk \
    && rm -rf /lib/x86_64-linux-gnu/libkeyutils.so.1*

FROM python:3.12-slim-bullseye as run

RUN apt update -qq \
    && apt install -y libbinutils libedit2 libncursesw5 wget awscli \
    && apt autoremove -y \
    && apt clean \
    && apt purge \
    && rm -rf /var/lib/apt/lists/*

# Copia de los componentes de Asterisk y picoTTS desde la etapa de desarrollo
COPY --from=dev /usr/local /usr/local
# Asterisk
COPY --from=dev /usr/lib/libasterisk* /usr/lib/
COPY --from=dev /etc/asterisk /etc/asterisk/
COPY --from=dev /var/lib/asterisk /var/lib/asterisk
COPY --from=dev /var/log/asterisk /var/log/asterisk
COPY --from=dev /var/spool/asterisk /var/spool/asterisk
COPY --from=dev /usr/lib/asterisk /usr/lib/asterisk/
COPY --from=dev /var/run/asterisk/ /var/run/asterisk/
# picoTTS
COPY --from=dev /usr/share/pico/lang/ /usr/share/pico/lang/
COPY --from=dev /usr/bin/pico2wave /usr/bin/pico2wave
# Sox
COPY --from=dev /usr/bin/sox /usr/bin/sox 
COPY --from=dev /usr/lib/mime/packages/sox /usr/lib/mime/packages/sox
COPY --from=dev /usr/bin/play /usr/bin/play
COPY --from=dev /usr/bin/rec /usr/bin/rec
COPY --from=dev /usr/bin/soxi /usr/bin/soxi

# Librerias de python, curl otras librerias necesarias
COPY --from=dev /usr/local/lib/python3.12/ /usr/local/lib/python3.12/
COPY --from=dev /usr/lib/x86_64-linux-gnu/ /usr/lib/x86_64-linux-gnu/

COPY source/astconf/* /etc/asterisk/
COPY source/scripts/* /opt/asterisk/scripts/
COPY source/agi-bin/* /var/lib/asterisk/agi-bin/
COPY build/docker-entrypoint.sh /docker-entrypoint.sh

RUN chmod 750 -R /var/spool/asterisk
RUN useradd -M -u 1000 omnileads
RUN chown -R omnileads.omnileads /var/lib/asterisk /etc/asterisk /opt/asterisk /usr/lib/asterisk /docker-entrypoint.sh /var/spool/asterisk /var/log/asterisk

EXPOSE 5060/udp

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD [""]
