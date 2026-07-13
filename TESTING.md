# Guía de Testing y Linting para ACD

Este documento describe cómo ejecutar los tests unitarios y el linter flake8 para el proyecto ACD.

## Requisitos Previos

Asegúrate de tener instalado Python 3.11 o superior y pip.

## Instalación de Dependencias

Instala las dependencias necesarias para testing y linting:

```bash
pip install -r build/requirements.txt
```

Esto instalará:
- `pytest` y `pytest-cov` para ejecutar tests y generar reportes de cobertura
- `flake8` para el análisis estático de código

## Ejecutar el Linter (Flake8)

Para verificar el estilo y calidad del código Python:

```bash
flake8 source/ari-app/ --config=.flake8 --statistics --count
```

### Configuración de Flake8

El proyecto usa un archivo `.flake8` en la raíz con las siguientes configuraciones:
- Longitud máxima de línea: 120 caracteres
- Complejidad máxima: 15
- Ignora ciertos errores comunes (E203, E501, W503, F401)

## Ejecutar Tests Unitarios

### Ejecutar todos los tests

```bash
pytest source/tests_unit/ -v
```

### Ejecutar tests con cobertura

```bash
pytest source/tests_unit/ -v --cov=source/ari-app --cov-report=term-missing --cov-report=html
```

Esto generará:
- Un reporte en la terminal con las líneas no cubiertas
- Un reporte HTML en `htmlcov/index.html`

### Ejecutar un test específico

```bash
pytest source/tests_unit/test_json_formatter.py -v
```

### Ejecutar tests con marcadores

```bash
# Solo tests unitarios
pytest -m unit

# Excluir tests lentos
pytest -m "not slow"
```

## Estructura de Tests

Los tests están organizados en `source/tests_unit/`:

```
source/tests_unit/
├── __init__.py
├── conftest.py              # Configuración compartida de pytest
├── test_json_formatter.py   # Tests para JsonFormatter
├── test_call_manager_init.py # Tests para inicialización de CallManager
└── test_pytest_setup.py     # Tests básicos de verificación
```

## Ejecutar en Contenedor Docker

### Opción 1: Usando Scripts (Recomendado)

El proyecto incluye scripts bash para ejecutar fácilmente los checks en un contenedor:

#### Ejecutar solo el linter:
```bash
./run_linter.sh
```

#### Ejecutar solo los tests:
```bash
./run_tests.sh
```

#### Ejecutar ambos (linter + tests):
```bash
./run_all_checks.sh
```

#### Opciones avanzadas:

Forzar reconstrucción de la imagen:
```bash
BUILD_IMAGE=true ./run_tests.sh
```

Usar una imagen personalizada:
```bash
IMAGE_NAME=mi-imagen:tag ./run_tests.sh
```

Pasar opciones adicionales a pytest:
```bash
PYTEST_OPTS="-v -k test_json" ./run_tests.sh
```

Pasar opciones adicionales a flake8:
```bash
FLAKE8_OPTS="--max-line-length=100" ./run_linter.sh
```

### Opción 2: Usando Docker Compose

El proyecto incluye un `docker-compose.test.yml` para ejecutar los checks:

#### Ejecutar linter:
```bash
docker-compose -f docker-compose.test.yml --profile lint up --build
```

#### Ejecutar tests:
```bash
docker-compose -f docker-compose.test.yml --profile pytest up --build
```

#### Ejecutar ambos:
```bash
docker-compose -f docker-compose.test.yml --profile test up --build
```

### Opción 3: Manualmente con Docker

Si prefieres ejecutar manualmente:

```bash
# Construir la imagen
docker build -t acd-test:latest -f Dockerfile .

# Ejecutar linter
docker run --rm \
  -v "$(pwd)/source:/opt/asterisk/source:ro" \
  -v "$(pwd)/.flake8:/opt/asterisk/.flake8:ro" \
  acd-test:latest \
  bash -c "pip install -r /requirements.txt && flake8 source/ari-app/ --config=.flake8 --statistics --count"

# Ejecutar tests
docker run --rm \
  -v "$(pwd)/source:/opt/asterisk/source:ro" \
  -v "$(pwd)/pytest.ini:/opt/asterisk/pytest.ini:ro" \
  -e PYTHONPATH=/opt/asterisk/ari-app \
  acd-test:latest \
  bash -c "pip install -r /requirements.txt && pytest source/tests_unit/ -v --cov=source/ari-app --cov-report=term-missing"
```

## Pipeline CI/CD

El pipeline de GitLab CI sigue el mismo patrón que el repositorio django. Por defecto solo construye la imagen Docker en ramas `develop` y `master-2.0`. Los stages de calidad y SonarQube se activan con variables de pipeline:

| Variable | Efecto |
|---|---|
| *(ninguna)* | Solo build de imagen (`container-image-dev`) |
| `SAST=true` | Análisis SonarQube + template SAST de GitLab |
| `FULL=true` | Flake8, pytest, SonarQube, SAST y build |

Stages cuando `FULL=true`:

1. **test** — `test:flake8` y `test:pytest` (genera `coverage.xml` para SonarQube)
2. **sonarqube** — `sonarqube-check` (importa cobertura desde `coverage.xml`)
3. **build** — `container-image-dev`

Para ejecutar el pipeline completo en un merge request hacia `develop` o `master-2.0`, lanzar el pipeline manualmente con `FULL=true` desde la UI de GitLab.

### SonarQube

Configuración en [`sonar-project.properties`](sonar-project.properties) (`projectKey=omnileads-acd`).

Requisitos en GitLab CI/CD (grupo omnileads, compartidos con django):

- `SONAR_HOST_URL` — URL del servidor SonarQube self-hosted (ej. `https://sonar.tudominio.com`)
- `SONAR_TOKEN` — token de usuario con permiso **Execute Analysis** sobre el proyecto

#### Crear el proyecto en SonarQube (administrador)

1. SonarQube → **Create Project** → **Manually**
2. **Project key:** `omnileads-acd` (debe coincidir exactamente con `sonar-project.properties`)
3. **Display name:** `OMniLeads ACD`
4. **Main branch:** `develop` (o la rama principal del repo)

#### Token para CI

1. SonarQube → avatar (arriba derecha) → **My Account → Security → Generate Tokens**
2. Nombre: `gitlab-ci-acd` (o reutilizar el token global que usa django)
3. Tipo: **Global Analysis Token** (recomendado) o **User Token** con permiso de análisis
4. Si el token es por proyecto: en **Project Settings → Permissions**, asegurar que el usuario del token tenga **Execute Analysis** en `omnileads-acd`

#### Variables en GitLab

Grupo `omnileads` → **Settings → CI/CD → Variables**:

| Variable | Valor |
|---|---|
| `SONAR_HOST_URL` | URL de tu SonarQube (no SonarCloud) |
| `SONAR_TOKEN` | Token generado arriba |

Si la variable está marcada **Protected**, solo estará disponible en ramas protegidas.

#### Troubleshooting

| Síntoma | Causa probable | Solución |
|---|---|---|
| `SONAR_TOKEN no está definido` | Variable Protected en rama no protegida | Desmarcar *Protected* o proteger la rama |
| `Project not found` | Proyecto no creado en SonarQube | Crear `omnileads-acd` manualmente (pasos arriba) |
| `Not authorized` | Token sin permiso en el proyecto | Global Analysis Token o permisos en Project Settings |

## Agregar Nuevos Tests

Para agregar nuevos tests:

1. Crea un archivo `test_*.py` en `source/tests_unit/`
2. Importa las clases/funciones a testear desde `source/ari-app/`
3. Usa fixtures de `conftest.py` cuando sea posible
4. Ejecuta los tests localmente antes de hacer commit

Ejemplo:

```python
"""Tests para mi_nuevo_modulo."""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ari-app'))

from mi_modulo import MiClase

class TestMiClase:
    def test_metodo_basico(self):
        obj = MiClase()
        assert obj.metodo() == resultado_esperado
```

## Troubleshooting

### Error: "Module not found" al ejecutar tests

Asegúrate de que el path esté configurado correctamente. Los tests agregan `source/ari-app/` al `sys.path` para poder importar los módulos.

### Error: "Redis connection failed" en tests

Los tests usan mocks de Redis, no necesitas una instancia real de Redis corriendo.

### Los tests fallan en CI pero pasan localmente

Verifica que las versiones de las dependencias sean las mismas. El CI usa las versiones especificadas en `build/requirements.txt`.

## Comportamiento y pruebas de transferencias ciegas (BlindToAgent)

Esta sección describe el comportamiento esperado y cómo verificar manualmente el flujo de hangup en transferencias ciegas implementado en la app ARI del ACD.

### Resumen de comportamiento

- **Escenario base**: llamada manual `Agente A ↔ PSTN` (cliente).
- **Acción**: el agente A ejecuta una transferencia ciega (`BlindToAgent`) hacia el agente B.
- **Resultado esperado**:
  - Cuando la pierna de transferencia (`TransferLegStart`) se completa, el contexto de llamada marca `is_transferred=True` y `agent_channel` pasa a ser el canal del agente B.
  - Si el **agente B (destino de la transferencia) cuelga**, el manejador `TransferManager.on_transfer_target_hangup()`:
    - Localiza el contexto por canal del agente B.
    - Verifica que la llamada es **manual** y que la transferencia está marcada como completada.
    - **Cuelga explícitamente el leg PSTN** asociado (`pstn_channel` / `uniqueid_pstn`).
    - **Intenta destruir el bridge** asociado. Si el bridge ya no existe, se loguea el caso sin lanzar error.
  - El cliente PSTN **no debe quedar aislado** en el bridge después de que cuelga el agente B.

### Logs relevantes

En el módulo `transfer.py` (`TransferManager`), se generan los siguientes logs clave:

- Al detectar el hangup del agente destino con transferencia ciega completada:

  - `📞 TransferTargetHangup: Detectado hangup de agente destino ... pstn_channel=..., bridge_id=...`

- Al colgar el leg PSTN:

  - `✅ TransferTargetHangup: PSTN leg ... colgado tras hangup de agente destino ...`
  - o bien  
  - `⚠️ TransferTargetHangup: hangup_channel retornó False para PSTN leg ... (puede que ya no exista)`

- Al destruir el bridge:

  - `✅ TransferTargetHangup: Bridge ... destruido tras hangup de agente destino ...`
  - o bien  
  - `⚠️ TransferTargetHangup: destroy_bridge retornó False para bridge ... (puede que ya no exista)`

- Para casos donde **no** se aplica la política de transferencia ciega:

  - Canal que no pertenece a llamada manual conocida:

    - `TransferTargetHangup: Canal ... no pertenece a llamada manual conocida; no se aplica política de transferencia ciega`

  - Canal que ya no está asociado al contexto:

    - `TransferTargetHangup: Canal ... ya no está asociado a call_id=...; ignorando hangup para evitar usar contexto obsoleto`

  - Hangup que no cumple condiciones de transferencia ciega (por ejemplo, no está `is_transferred` o no es el `agent_channel` actual):

    - `TransferTargetHangup: Hangup en canal ... para call_id=... no cumple condiciones de transferencia ciega (is_transferred=..., is_current_agent_channel=...); no se colgará PSTN ni se destruirá bridge`

### Escenarios de prueba manual

#### Escenario A: llamada manual con transferencia ciega y hangup del agente B

1. Originar una **llamada manual** A → PSTN (cliente).
2. Confirmar en logs que se crea el bridge principal y que se registran `agent_channel` y `pstn_channel` en el contexto.
3. Desde el agente A, ejecutar una **transferencia ciega** (`BlindToAgent`) hacia el agente B.
4. Verificar en logs de `TransferLegStart` que:
   - Se marca `ctx.is_transferred=True`.
   - `ctx.agent_channel` pasa a ser el canal de B.
5. Hacer que el **agente B cuelgue**.
6. Verificar en logs:
   - Aparición de `📞 TransferTargetHangup: Detectado hangup de agente destino ...`.
   - Posterior `✅ TransferTargetHangup: PSTN leg ... colgado ...` (o el warning si ya no existía).
   - Intento de destrucción del bridge (`✅ TransferTargetHangup: Bridge ... destruido ...`).
7. Confirmar por trazas de Asterisk/ARI que **el cliente PSTN cuelga** y que **no quedan canales en el bridge**.

#### Escenario B: llamada manual sin transferencia

1. Originar llamada manual A → PSTN.
2. Sin realizar transferencia, hacer que el agente A cuelgue.
3. Verificar que **NO** aparecen logs `TransferTargetHangup` relacionados con esa llamada.
4. Confirmar que el flujo de finalización de llamada sigue siendo el existente (manejado por los handlers manuales estándar).

#### Escenario C: escenarios fuera de política de transferencia ciega

1. Repetir el flujo de llamada manual, pero provocar otros tipos de hangup:
   - Hangup del cliente PSTN primero.
   - Hangup de un canal que ya no está asociado al contexto (por ejemplo, simulando condiciones de carrera).
2. Confirmar que:
   - Los logs `TransferTargetHangup: ... no se aplica política de transferencia ciega` aparecen cuando corresponde.
   - No se intenta colgar PSTN ni destruir bridges en escenarios que no cumplan la condición `is_transferred=True` + canal de agente destino activo.

## Timeouts y logging en `ari-app`

- **Timeouts ARI HTTP**:
  - `ARI_CONNECT_TIMEOUT` (env): tiempo máximo para establecer conexión HTTP con ARI (default: 3s).
  - `ARI_READ_TIMEOUT` (env): tiempo máximo de espera de respuesta HTTP de ARI (default: 15s).
- **Timeouts de originación de llamadas**:
  - `DEFAULT_ORIGINATE_TIMEOUT` (env): timeout por defecto para originaciones ARI (default: 30s).  
    Se usa cuando no se especifica un `attempt_timeout` válido.
  - `TRANSFER_TIMEOUT` (env): timeout estándar para originaciones de piernas de transferencia (default: 30s).
  - `CONSULT_TIMEOUT` (env): timeout estándar para originaciones de piernas de consulta (default: 45s).
- **Reglas de `attempt_timeout`**:
  - Si el comando incluye `attempt_timeout` (en el payload raíz o dentro de `metadata`) y es un entero positivo, se usa ese valor tanto para:
    - El campo `attempt_timeout` en `metadata` (para trazabilidad).
    - El `timeout` efectivo en las originaciones hacia el agente cuando aplica.
  - Si `attempt_timeout` no es válido (no numérico o ≤ 0), se ignora y se usa `DEFAULT_ORIGINATE_TIMEOUT`.  
    Se genera un log `warning` con el valor rechazado.
- **Estandarización de logging**:
  - Los módulos nuevos y refactorizados usan loggers creados como `logging.getLogger(__name__)`, de modo que:
    - El nombre de logger coincide con el módulo Python.
    - Es fácil filtrar por componente (`ari_app.transfer`, `ari_app.recording_client`, etc.).
  - Los mensajes clave relacionados con timeouts y llamadas incluyen siempre el valor de `timeout` efectivo en segundos.
