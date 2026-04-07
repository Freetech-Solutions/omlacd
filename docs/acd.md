# Documentación del Componente ACD en OMniLeads

Este documento explica en detalle la arquitectura y flujos de trabajo del componente **ACD (Automatic Call Distributor)** dentro del stack de OMniLeads. Este componente está desarrollado principalmente en Python interactuyendo con **Asterisk** a través de **ARI (Asterisk REST Interface)**, y se apoya en **Redis** para control de estado distribuido y **Gearman** para procesamiento de colas asíncronas.

---

## 1. Variables de Entorno

La configuración del componente recae sobre un modelo validado estrictamente (`pydantic-settings`) al inicio de la aplicación en `config.py`. Sigue la metodología Twelve-Factor App.

Las **variables críticas** (sin valor por defecto) que detendrán la aplicación si faltan son:
- `ARI_USER` (alias `ASTERISK_USER`): Usuario para autenticarse en ARI.
- `ARI_PASSWORD` (alias `ASTERISK_PASS`): Contraseña de ARI.
- `ARI_APP` (alias `ASTERISK_APP`): Nombre de la aplicación Stasis dentro de Asterisk.
- `ARI_URL`: URL completa de la API ARI (ej. `http://host:8088`).
- `REDIS_URL`: URL del servidor Redis para almacenamiento de estado y locks (ej. `redis://host:6379/0`).
- `SIP_TRUNK`: Nombre del troncal SIP para cursar llamadas salientes hacia la PSTN.
**Algunas variables opcionales importantes:**
- `LOG_LEVEL`: Nivel de detalle de los logs (INFO, DEBUG, WARNING, ERROR).
- `WEBRTC_TRUNK`: Troncal por defecto de los agentes WebRTC (ej. `kamailio-webrtc`).
- `GEARMAN_SERVERS`: Lista de servidores Gearman (ej. `gearman:4730`).
- `RECORDING_ENABLED`, `RECORDING_BASE_PATH`, `BUCKET_NAME`, `BUCKET_ACCESS_KEY_ID`, `BUCKET_SECRET_ACCESS_KEY`: Definen la política de grabaciones y sincronización del WAV local con almacenamiento S3 compatible.

---

## 2. Aplicación ARI (`main.py`) y Arquitectura Interna

El archivo `main.py` es el punto de entrada de la aplicación. Se divide lógicamente en los siguientes componentes:

1. **Inyección de Dependencias (`ACDContainer`)**: Utiliza `dependency_injector` para proveer instancias singletons a toda la aplicación (clientes de Redis, Gearman, estado distribuido, servicios de agentes).
2. **Conexión WebSocket (`start_websocket`)**: Mantiene una conexión persistente bidireccional contra Asterisk (`/ari/events`). Escucha eventos y los parsea usando validadores Pydantic.
3. **Control de Saturación (`CircuitBreaker`) y Hilo Consumidor**:
   - Los mensajes validados no se procesan en el hilo del WebSocket. Entran una cola (`event_queue`).
   - Un hilo dedicado (`_event_worker`) consume secuencialmente los eventos FIFO. Esto asegura orden y previene sobrecarga de recursos o race conditions descontrolados. 
   - Un mecanismo de _Circuit Breaker_ descarta ventos si detecta fallos abruptos en cascada o saturación, protegiendo así al componente de colapsar.
4. **Router (`router.py`)**: 
   - Centraliza cada evento ARI e inspecciona atributos para derivarlo a un _Handler_ específico (`handlers/inbound.py`, `handlers/campaign.py`, etc.).
5. **Listeners Auxiliares**:
   - `CommandListener`: Escucha comandos interactivos desde Redis.
   - `GearmanListener`: Escucha tareas asignadas por el backend legacy Django vía Gearman.

---

## 3. Archivos de Configuración (`.conf`)

Estos residen generalmente en el directorio `source/astconf/`, y configuran el comportamiento interno del proceso nativo de Asterisk antes de delegar el control a nuestra aplicación ARI.

- **`extensions.conf`**: Define el Dialplan básico de la plataforma. Crea contextos como `[from-pstn]`, `[from-pbx]` y el `[default]`. Estos derivan las llamadas entrantes genéricas hacia macros y componentes donde, en última instancia, se invoca a la aplicación Stasis de ARI para ceder el control al ACD en Python.
- Los archivos modulares (como `oml_extensions_globals.conf`, `oml_extensions.conf`, y sus sufijos `_override.conf`) permiten escalar la configuración de PBX del sistema de manera limpia.

---

## 4. Flujo: Cómo Sucede una Llamada Inbound

Este caso de uso es manejado íntegramente por `handlers/inbound.py`:

1. **Ingreso a Stasis**: El canal PSTN del cliente llama a una ruta entrante y es enviado a la aplicación Stasis ARI. Dispara `StasisStart`.
2. **Creación del Entorno**: 
   - `InboundCallHandler` parsea variables como `id_camp` (Campaña), `tel_customer`.
   - Crea un puente mezclador (_mixing bridge_) usando `CallActionService`.
   - Hace un `Answer` de la línea PSTN y la adhiere al `bridge_id`.
   - Empieza a reproducir música en espera (MOH).
3. **Registro de Estado**: Se instancia el objeto `CallContext` y se persiste unívocamente en **Redis** mediante un lock seguro, evitando race conditions.
4. **Distribución (Ringing)**: Llama a `DistributionService` para comenzar la búsqueda y discado (originate) de agentes disponibles basados en las estrategias de colas y pausas.
5. **Respuesta del Agente (`on_agent_stasis_start`)**:
   - Cuando el canal del agente atiende, entra a Stasis. Se frena la música en espera.
   - El canal del agente se junta al `bridge_id` preexistente (mezclándolo con el canal PSTN).
   - Se actualiza el estado ONCALL del agente en Redis y envía los reportes finales de answer time al backend.

---

## 5. Flujo: Cómo Sucede una Llamada Dialer (Campañas Progresivas)

Manejado por `handlers/campaign.py`. Es lo opuesto a manual, el sistema llama primero al contacto:

1. **Originate a PSTN**: El sistema inicia un discado autómata hacia el número del cliente. 
2. **Evaluación de Contestador (AMD)**: Si la máquina de campaña discadora tiene Análisis de Máquina Contestadora (AMD) activado, cuando el cliente atiende se envía el canal a un dialplan `[amd]`.
3. **Cliente Atiende y Pasa a Stasis**: 
   - Solo si el motor califica como `HUMAN` (o si no tiene AMD), se dispara el evento `StasisStart` en el handler.
   - Crea el `CallContext`, el `bridge` y arroja de antemano el MOH para mantener al cliente esperando.
4. **Distribución al Agente**: Idéntico al caso de Inbound. El `DistributionService` notifica a los agentes de la campaña y los hace sonar. Al contestar, el software detiene el MOH y junta el canal del agente humano con el PSTN del cliente en el bridge de audio.

---

## 6. Flujo: Cómo Sucede una Llamada Manual

Manejada en `handlers/manual.py`, el acercamiento cambia; el agente toma la iniciativa.

1. **StasisStart del Agente**: En este caso, el canal originante es primero el del agente WebRTC y dispara el inicio en la app.
2. **Creación de Escenario Inicial**:
   - Se crea inmediatamente el `mixing bridge`.
   - Se agrega al agente de manera directa al bridge. 
3. **Orígen a PSTN (`CallActionService.dial_pstn`)**: 
   - A diferencia de las entrantes, ahora es la App ARI la que usa la API para originar proactivamente la llamada asíncrona hacia la red de calle (PSTN/Troncal externo).
4. **Acoplamiento**: Al responder el extremo PSTN del lado de la calle, es instertado al `bridge` donde se encontraba esperando silenciado el agente, entablándose la comunicación manual bidireccional. 

---

## 7. Manejo y Reporte de las Grabaciones (Bucket)

El gestor está aislado en `handlers/recording.py` y `recording_client.py`:

1. **Monitoreo (`ChannelEnteredBridge`)**: Al unirse piezas al puente, `RecordingEventHandler` determina si bajo las premisas de la llamada procede grabar. Envía comando a Asterisk para comenzar mixeo y grabar localmente.
2. **Extracción (`RecordingFinished`)**: El fin de puente/llamada detiene la grabación. ARI notifica ésto e ingresa la ruta del archivo WAV. Al recibir el evento, el job aterriza en `RecordingManager`.
3. **Subida y Optimización Asíncrona**: En un *ThreadPool* de manera _background_, ocurren los siguientes pasos atómicos:
   - Se invoca al `s3_client` y se sube velózmente el el WAV crudo a un bucket compatible con Object Storage si las variables están configuradas.  
   - Si la subida s3 es correcta, **se borra el .wav de disco local**.
   - Se encola el procesamiento enviando un job de Gearman `tel-callrec-compressor` al viejo backend (indicándole además de la metadata, el `s3_wav_key` para descargalo si se subió a S3) asegurando que el ACD de Pyhon se mantenga liguero.

---

## 8. Logs y Reportes para el Legacy Omnidialer (Backend Django)

La clase `ACDReporter` localizada en `reporter.py` asume la responsabilidad de traducir todo el ciclo de estados a cargas útiles analíticas listas para SQL, a fin de enviarlas al legacy backend.

1. **Uso de Gearman**: El ACD envía los logs vía Gearman (`submit_job("acd-log-processor", ...)`).
2. **Métodos Diferenciados**: 
   - **`log_dial` / `log_queue`**: Emiten notificaciones para alimentar reportes en tiempo real y dashboards de colas de llamadas (`reportes_app_interaction_log`).
   - **`log_connect` / `log_segment_end`**: Calculan numéricamente los timestamps definitivos (`bridge_wait_time`, `duracion_llamada`, `agent_duration`). Asignan un `hangup_cause` estándar (ej. `CONGESTION`, `NOANSWER`, `EXIT_ANSWERED`). Adhieren las nomenclaturas de "quien cortó" (`quien_corto`).
   - **`log_transfer`**: Modela a nivel entidad transferencias SIP asistida/ciega que afectan la tabla `reportes_app_transferlog`.
3. **Compatibilidad estricta**: Los payloads normalizan y transforman cadenas vacías o `-1` a `None` para ser perfectamente tolerables en el schema actual de PostgreSQL de OMniLeads.
