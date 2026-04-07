#!/bin/bash
# Script para ejecutar tanto linter como tests en un contenedor Docker

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${BLUE}** [OMniLeads ACD] Ejecutando Todas las Verificaciones **${NC}"
echo ""

# Ejecutar linter primero
echo -e "${YELLOW}=== Ejecutando Linter (Flake8) ===${NC}"
"$SCRIPT_DIR/run_linter.sh"
LINTER_EXIT=$?

echo ""
echo -e "${YELLOW}=== Ejecutando Tests Unitarios ===${NC}"
"$SCRIPT_DIR/run_tests.sh"
TESTS_EXIT=$?

echo ""
echo -e "${BLUE}** Resumen de Resultados **${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ $LINTER_EXIT -eq 0 ]; then
    echo -e "${GREEN}✅ Linter: PASSED${NC}"
else
    echo -e "${RED}❌ Linter: FAILED${NC}"
fi

if [ $TESTS_EXIT -eq 0 ]; then
    echo -e "${GREEN}✅ Tests: PASSED${NC}"
else
    echo -e "${RED}❌ Tests: FAILED${NC}"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Salir con código de error si alguna verificación falló
if [ $LINTER_EXIT -ne 0 ] || [ $TESTS_EXIT -ne 0 ]; then
    echo -e "${RED}❌ Algunas verificaciones fallaron${NC}"
    exit 1
else
    echo -e "${GREEN}✅ Todas las verificaciones pasaron${NC}"
    exit 0
fi

