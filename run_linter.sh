#!/bin/bash
# Script para ejecutar flake8 linter en un contenedor Docker

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

IMAGE_NAME="${IMAGE_NAME:-freetechsolutions/asterisk:test}"
CONTAINER_NAME="acd-linter-$(date +%s)"

echo -e "${GREEN}** [OMniLeads ACD] Ejecutando Flake8 Linter en Contenedor **${NC}"

# Verificar si Docker está disponible
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker no está instalado o no está en el PATH${NC}"
    exit 1
fi

# Construir la imagen si no existe o si se fuerza
if [ "${BUILD_IMAGE:-false}" = "true" ] || ! docker image inspect "$IMAGE_NAME" &> /dev/null; then
    echo -e "${YELLOW}Construyendo imagen Docker...${NC}"
    docker build -t "$IMAGE_NAME" -f Dockerfile .
fi

# Crear contenedor temporal y ejecutar linter
echo -e "${GREEN}Ejecutando flake8 en contenedor...${NC}"

# Opciones adicionales de flake8
FLAKE8_OPTS="${FLAKE8_OPTS:---statistics --count}"

docker run --rm \
    --name "$CONTAINER_NAME" \
    -v "$(pwd)/source/ari-app:/opt/asterisk/ari-app:ro" \
    -v "$(pwd)/.flake8:/opt/asterisk/.flake8:ro" \
    "$IMAGE_NAME" \
    bash -c "
        cd /opt/asterisk && \
        pip install -q -r /requirements.txt && \
        flake8 ari-app/ --config=.flake8 $FLAKE8_OPTS
    "

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Linter ejecutado exitosamente - No se encontraron errores${NC}"
else
    echo -e "${RED}❌ Linter encontró errores (código de salida: $EXIT_CODE)${NC}"
fi

exit $EXIT_CODE

