# Certificados TLS de desarrollo (Asterisk)

Los archivos `cert.pem` y `key.pem` son certificados **autofirmados solo para desarrollo local**
y el build de imagen Docker. No deben usarse en producción.

Para regenerarlos:

```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 3650 -nodes -subj "/CN=acd-dev.local"
```

En producción, montar certificados reales vía secretos/volúmenes del entorno de despliegue.
