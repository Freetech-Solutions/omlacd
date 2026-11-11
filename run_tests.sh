#!/bin/bash
# Ejecutar tests unitarios del ACD en un contenedor Docker.
#
# Uso:
#   ./run_tests.sh                         # suite aislada por archivo + workers
#   ./run_tests.sh source/tests_unit/test_queue_events.py
#   ./run_tests.sh --help
#
# Importante: varios tests hacen sys.modules['pydantic'|'state'|'redis']=...
# a nivel de módulo. En un solo proceso eso contamina al resto (PydanticUserError,
# redis sin RedisError, utils mockeado, etc.). Por eso la suite completa corre
# cada test_*.py en un proceso aparte (subprocess), con coverage --cov-append.
#
# Rebuild imagen: BUILD_IMAGE=true ./run_tests.sh

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

IMAGE_NAME="${IMAGE_NAME:-omnileads-acd-test:latest}"
CONTAINER_NAME="acd-tests-$(date +%s)"
PLATFORM_ARGS=()
if [ -n "${DOCKER_PLATFORM:-}" ]; then
    PLATFORM_ARGS=(--platform "$DOCKER_PLATFORM")
elif [ "$(uname -m)" = "arm64" ] || [ "$(uname -m)" = "aarch64" ]; then
    PLATFORM_ARGS=(--platform linux/amd64)
fi

case "${1:-}" in
    --help|-h)
        sed -n '2,16p' "$0"
        exit 0
        ;;
esac

echo -e "${GREEN}** [OMniLeads ACD] Tests unitarios en contenedor **${NC}"

if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker no está instalado o no está en el PATH${NC}"
    exit 1
fi

if [ "${BUILD_IMAGE:-false}" = "true" ] || ! docker image inspect "$IMAGE_NAME" &> /dev/null; then
    echo -e "${YELLOW}Construyendo imagen Docker (primera vez o BUILD_IMAGE=true)...${NC}"
    docker build "${PLATFORM_ARGS[@]}" -t "$IMAGE_NAME" -f Dockerfile .
fi

echo -e "${GREEN}Ejecutando tests (código montado; un proceso por archivo)...${NC}"

# Montajes:
# - /opt/asterisk/ari-app  → PYTHONPATH (igual que runtime)
# - /opt/asterisk/source   → tests que resuelven ../ari-app, ../workers, etc.
# No re-pip: deps ya en imagen; git se purgó tras el build.

set +e
# -i: el heredoc (script Python) debe llegar a stdin del proceso en el contenedor.
docker run --rm -i \
    "${PLATFORM_ARGS[@]}" \
    --name "$CONTAINER_NAME" \
    -v "$(pwd)/source:/opt/asterisk/source:ro" \
    -v "$(pwd)/source/ari-app:/opt/asterisk/ari-app:ro" \
    -v "$(pwd)/source/workers:/opt/asterisk/workers:ro" \
    -v "$(pwd)/pytest.ini:/opt/asterisk/pytest.ini:ro" \
    -e PYTHONPATH=/opt/asterisk/ari-app \
    -e PYTHONDONTWRITEBYTECODE=1 \
    -e ARI_USER=test \
    -e ARI_PASSWORD=test \
    -e ARI_APP=test_app \
    -e ARI_URL=http://127.0.0.1:8088 \
    -e REDIS_URL=redis://127.0.0.1:6379/0 \
    -w /opt/asterisk \
    "$IMAGE_NAME" \
    python - "$@" <<'PY'
import os
import subprocess
import sys
from pathlib import Path

args = sys.argv[1:]
root = Path("/opt/asterisk")
tests_dir = root / "source" / "tests_unit"
env = os.environ.copy()
env["PYTHONPATH"] = "/opt/asterisk/ari-app"
env["PYTHONDONTWRITEBYTECODE"] = "1"
# Settings() de config.py exige estos env en import
env.setdefault("ARI_USER", "test")
env.setdefault("ARI_PASSWORD", "test")
env.setdefault("ARI_APP", "test_app")
env.setdefault("ARI_URL", "http://127.0.0.1:8088")
env.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

base_pytest = [
    sys.executable, "-m", "pytest",
    "-q", "--tb=line",
    # Anular addopts de pytest.ini (incluye --cov ruidoso por archivo)
    "-o", "addopts=",
    "-W", "ignore::pydantic.warnings.PydanticDeprecatedSince20",
]

def run_one(targets, *, cov_append: bool) -> int:
    cmd = list(base_pytest)
    cmd.extend(targets)
    # Cobertura sobre el tree montado en source/ (coincide con pytest.ini histórico)
    cmd.extend(["--cov=source/ari-app", "--cov-report="])
    if cov_append:
        cmd.append("--cov-append")
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=str(root), env=env).returncode

failed = []
if args:
    # Target(s) explícitos: un solo proceso (comportamiento clásico).
    rc = run_one(args, cov_append=False)
    if rc != 0:
        failed.append(" ".join(args))
else:
    files = sorted(tests_dir.glob("test_*.py"))
    if not files:
        print("No se encontraron test_*.py", file=sys.stderr)
        sys.exit(2)
    # Limpiar coverage previa
    cov_file = root / ".coverage"
    if cov_file.exists():
        cov_file.unlink()
    for i, f in enumerate(files):
        rel = str(f.relative_to(root))
        rc = run_one([rel], cov_append=(i > 0))
        if rc != 0:
            failed.append(rel)
            print(f"FAIL {rel} (exit {rc})", flush=True)
        else:
            print(f"OK   {rel}", flush=True)

    # Reporte de coverage consolidado
    subprocess.run(
        [sys.executable, "-m", "coverage", "report", "--precision=0"],
        cwd=str(root), env=env, check=False,
    )
    subprocess.run(
        [sys.executable, "-m", "coverage", "html", "-d", "htmlcov"],
        cwd=str(root), env=env, check=False,
    )
    subprocess.run(
        [sys.executable, "-m", "coverage", "xml", "-o", "coverage.xml"],
        cwd=str(root), env=env, check=False,
    )

    # Worker event_stream (unittest, sin pytest pollution)
    print("\n=== workers/tests_event_stream ===", flush=True)
    rc = subprocess.run(
        [sys.executable, "-m", "unittest", "tests_event_stream", "-v"],
        cwd="/opt/asterisk/workers",
        env={**env, "PYTHONPATH": "/opt/asterisk/workers"},
    ).returncode
    if rc != 0:
        failed.append("workers/tests_event_stream")

if failed:
    print("\nArchivos con fallo:", file=sys.stderr)
    for name in failed:
        print(f"  - {name}", file=sys.stderr)
    sys.exit(1)
print("\nTests OK")
sys.exit(0)
PY
EXIT_CODE=$?
set -e

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}Tests OK${NC}"
else
    echo -e "${RED}Tests fallaron (exit ${EXIT_CODE})${NC}"
fi

exit $EXIT_CODE
