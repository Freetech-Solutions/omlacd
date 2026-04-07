#!/bin/bash
# Script para ejecutar tests unitarios en un contenedor Docker

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

IMAGE_NAME="${IMAGE_NAME:-freetechsolutions/asterisk:test}"
CONTAINER_NAME="acd-tests-$(date +%s)"

echo -e "${GREEN}** [OMniLeads ACD] Ejecutando Tests Unitarios en Contenedor **${NC}"

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

# Crear contenedor temporal y ejecutar tests
echo -e "${GREEN}Ejecutando tests en contenedor...${NC}"

# Opciones adicionales de pytest
PYTEST_OPTS="${PYTEST_OPTS:--v --cov=ari-app --cov-report=term-missing}"

docker run --rm \
    --name "$CONTAINER_NAME" \
    -v "$(pwd)/source/tests_unit:/opt/asterisk/source/tests_unit:ro" \
    -v "$(pwd)/source/ari-app:/opt/asterisk/ari-app:ro" \
    -v "$(pwd)/pytest.ini:/opt/asterisk/pytest.ini:ro" \
    -e PYTHONPATH=/opt/asterisk/ari-app \
    "$IMAGE_NAME" \
    bash -c "
        cd /opt/asterisk && \
        pip install -q -r /requirements.txt && \
        pytest source/tests_unit/ $PYTEST_OPTS
    "

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Tests ejecutados exitosamente${NC}"
else
    echo -e "${RED}❌ Tests fallaron con código de salida: $EXIT_CODE${NC}"
fi

exit $EXIT_CODE

