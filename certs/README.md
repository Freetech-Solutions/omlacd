# Certificados TLS (Asterisk)

PJSIP espera:

- `/etc/asterisk/certs/cert.pem`
- `/etc/asterisk/certs/key.pem`

**Producción:** montar certificados reales vía secretos/volúmenes del entorno de despliegue. No bakear claves en la imagen.

**Desarrollo:** si faltan al arrancar, el `docker-entrypoint.sh` genera un par autofirmado efímero en runtime (no queda en la capa de la imagen).

Para generarlos localmente (fuera de Docker):

```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 3650 -nodes -subj "/CN=acd-dev.local"
```
