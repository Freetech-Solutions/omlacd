# Voicebots por AudioSocket en ACD / Stasis

Documentación de la integración **implementada**: Asterisk ARI `externalMedia` (AudioSocket) + servicio `audiosocket` (CES) + handoff por Redis, conviviendo con voicebots SIP / SIP REFER.

Documento hermano del componente: [`components-git-repo/audiosocket/README.md`](../../audiosocket/README.md).

---

## 1. Resumen

La plataforma soporta dos transportes de voicebot sin cambiar el core de orquestación (Stasis/ARI + Redis):

| Transporte | Media | Control / handoff |
|------------|--------|-------------------|
| **SIP** (legado) | Originate PJSIP + headers `X-OML-*` | Preferido: SIP REFER → `sip_refer_listener` |
| **AudioSocket** | ARI `externalMedia` (`encapsulation=audiosocket`) | Redis `action=voicebot_handoff` → `begin_voicebot_handoff` |

- Stasis/ARI sigue siendo la autoridad de bridge, estado (`CallRegistry`) y distribución.
- El servicio **`audiosocket`** es el *Bot Gateway*: termina TCP AudioSocket, habla con Google CES (WSS) y publica comandos al ACD.
- Variables de negocio no viajan en el protocolo AudioSocket: se publican en Redis `OML:AUDIOSOCKET:{uuid}`.

```text
PSTN ──► acd-app (bridge / CallRegistry)
              │
              ├─ SIP voicebot ──────────► proveedor SIP (REFER opcional)
              │
              └─ externalMedia TCP ─────► audiosocket:9092
                                              │
                         Redis sesión         │ WSS CES
                    OML:AUDIOSOCKET:{uuid}    ▼
                                              Google CES
                                              │
                         Redis Pub/Sub        │ end_session escalado
                    acd:commands:global ◄─────┘
                         voicebot_handoff
                              │
                              ▼
                    DistributionService.begin_voicebot_handoff
                    (MOH + start_distribution humanos)
```

---

## 2. Selección de transporte

En el loop de voicebot (`DistributionService`), por agente se lee configuración Redis (`VOICEBOT_TRANSPORT`, etc.):

- `VOICEBOT_TRANSPORT=audiosocket` → `CallManager.dial_voicebot_audiosocket` (requiere `AUDIOSOCKET_ADDR`).
- Cualquier otro / default SIP → `dial_voicebot_with_headers` (requiere `VOICEBOT_ADDR` / host externo).

Código: `source/ari-app/services/distribution_service.py` (rama `voicebot_transport == "audiosocket"`).

---

## 3. Componentes y código

| Pieza | Rol | Ubicación |
|-------|-----|-----------|
| Asterisk | Bridge, canales, `externalMedia` / PJSIP | `chan_audiosocket` / ARI |
| acd-app | Orquestación, dial bot, handoff, distribución | `source/ari-app/` |
| `CallManager.dial_voicebot_audiosocket` | Escribe sesión Redis + `POST externalMedia` | `services/call_manager.py` |
| `CommandDispatcher` | Acción `voicebot_handoff` | `services/command_dispatcher.py` |
| `DistributionService.begin_voicebot_handoff` | Saca pierna bot, MOH, `start_distribution` | `services/distribution_service.py` |
| `SipReferListener` | Handoff SIP legado | `sip_refer_listener.py` |
| Config ACD | `AUDIOSOCKET_ADDR`, TTL sesión | `config.py` |
| Clave Redis sesión | `OML:AUDIOSOCKET:{uuid}` | `constants.py` → `RedisKeys.audiosocket_session` |
| Servicio audiosocket | Gateway TCP + CES + webhook Django + publish Redis | `components-git-repo/audiosocket/` |
| Tests ACD handoff | Unitarios handoff | `source/tests_unit/test_voicebot_handoff.py` |

No existe un `Bot Session Manager` / `BotAdapter` abstracto separado: la selección de transporte está en el loop de voicebot; la sesión AudioSocket se modela con el canal externalMedia + metadata en `pending_dial_store` + flags en `CallContext` (`is_voicebot`, `voicebot_transfer_waiting`, `voicebot_leg_end_ts`, etc.).

---

## 4. Flujo AudioSocket (llamada)

### 4.1 Alta de pierna bot

1. Cliente en Stasis; campaña con agente `VOICEBOT=1` y `VOICEBOT_TRANSPORT=audiosocket`.
2. `dial_voicebot_audiosocket`:
   - Genera `session_uuid`.
   - `SETEX OML:AUDIOSOCKET:{uuid}` (TTL `AUDIOSOCKET_SESSION_TTL_SEC`, default 3600) con:

     ```json
     {
       "X-OML-Contact-ID": "<id_customer>",
       "X-OML-Campaign-ID": "<id_camp>",
       "X-OML-Call-ID": "<callid>",
       "phone_number": "...",
       "agent_id": "..."
     }
     ```

   - Registra `pending_dial_store` (externalMedia no manda `appArgs` en StasisStart).
   - ARI `externalMedia`:
     - `external_host` = `AUDIOSOCKET_ADDR` (ej. `audiosocket:9092`)
     - `encapsulation=audiosocket`, `transport=tcp`, `format=slin`, `direction=both`
     - `data=<session_uuid>`
3. El canal entra a Stasis, se agrega al bridge como pierna de “agente” voicebot.
4. `audiosocket` acepta TCP, lee `MSG_UUID` (16 bytes), carga Redis, conecta CES, envía saludo.

### 4.2 Media

- Protocolo AudioSocket: hangup `0x00`, UUID `0x01`, audio PCM LINEAR16 8 kHz `0x10` (tramas 320 B / 20 ms).
- El gateway agrupa ~100 ms hacia CES y pacea 20 ms hacia Asterisk.
- Vendor actual: `VOICEBOT_VENDOR=ces` (Google CES BidiRunSession WSS).

### 4.3 Fin de sesión CES → ACD

El adapter CES espera un evento estructurado `endSession` (también acepta fallback si el modelo **verbaliza** `end_session(...)` en el transcript).

| `session_escalated` | Comportamiento en `audiosocket` | Efecto en ACD |
|---------------------|----------------------------------|---------------|
| `true` | Login OML + calificación Django → `PUBLISH voicebot_handoff` → hangup pierna TCP | `begin_voicebot_handoff`: remueve/cuelga externalMedia, MOH, `start_distribution` humanos |
| `false` / ausente | Solo cierra pierna TCP (MSG_HANGUP + close) | Hangup de pierna voicebot; PSTN se limpia como fin de agente bot (sin transferencia) |

Orden crítico en escalación: **publicar `voicebot_handoff` antes** (o apenas) del hangup de la pierna, para que `voicebot_transfer_waiting` evite colgar el PSTN.

### 4.4 Calificación Django (antes del handoff)

Implementado en `audiosocket/oml_api.py`:

1. `POST /api/v1/login` (`OML_VOICEBOT_USERNAME` / `PASSWORD`, token cacheado).
2. `GET /api/v1/campaign/{id}/dispositionOptions/` → opción `name=GESTION_BOT` (configurable).
3. Contacto válido (`X-OML-Contact-ID` > 0) → `POST /api/v1/webhook/voicebot/`.
4. Contacto `-1` / ausente → `POST /api/v1/new_contact/disposition/` con `phone` de la sesión Redis.

Si falla el webhook o faltan env, se loguea warning y **igual** se publica `voicebot_handoff`.

---

## 5. Flujo SIP (legado, sin cambios disruptivos)

1. `dial_voicebot_with_headers` → originate SIP con headers de negocio.
2. Conversación en el mismo bridge.
3. Handoff típico: SIP REFER → `sip_refer_listener.py`:
   - destino agente → `TransferManager.blind_to_agent`
   - destino campaña → libera pierna bot, MOH, waiter + `voicebot_transfer_proceed` / distribución

AudioSocket **no** usa SIP REFER.

---

## 6. Contratos Redis (reales)

### 6.1 Sesión AudioSocket (ACD → gateway)

- **Clave:** `OML:AUDIOSOCKET:{session_uuid}`
- **Escritor:** `CallManager.dial_voicebot_audiosocket`
- **Lector:** `audiosocket.load_oml_session`
- **TTL:** `AUDIOSOCKET_SESSION_TTL_SEC`

Los `X-OML-*` se inyectan en CES (`remoteDialogflowQueryParameters`).

### 6.2 Handoff (gateway → ACD)

- **Canal:** `acd:commands:global` (override en gateway: `ACD_COMMANDS_CHANNEL`)
- **Payload implementado:**

```json
{"action": "voicebot_handoff", "call_id": "<X-OML-Call-ID>"}
```

- **Consumidor:** `CommandListener` → `CommandDispatcher` → `DistributionService.begin_voicebot_handoff(call_id)`

No se usan (hoy) acciones propuestas como `bot_handoff_queue` / `bot_session_end` / `bot_fallback_human`. El handoff AudioSocket es una sola acción: `voicebot_handoff`, siempre a distribución humana de la campaña del contexto.

### 6.3 Acciones SIP relacionadas (referencia)

Siguen vigentes para el flujo REFER: `voicebot_transfer_proceed`, waiters en `DistributionService`, etc. Son independientes del path AudioSocket.

---

## 7. `begin_voicebot_handoff` (detalle ACD)

1. Valida `CallContext` vivo, `bridge_id`, `campaign_id`.
2. Marca `voicebot_leg_end_ts`, `is_voicebot_transfer=True`, limpia pierna agente, `voicebot_transfer_waiting=True`.
3. `remove_channel_from_bridge` + `hangup` de la pierna bot (si aún existe; tolera race con cierre TCP).
4. `release_voicebot_call` / DECR `VOICEBOT-CALLS`.
5. MOH en el bridge del PSTN.
6. Limpia `voicebot_transfer_waiting` y llama `start_distribution` a humanos (misma campaña / cola efectiva).

Handlers de campaña/inbound distinguen post-handoff (`is_voicebot_transfer`, `voicebot_leg_end_ts`) para reporting y no tratar el hangup del bot como fin total de la llamada.

---

## 8. Configuración

### 8.1 ACD (`acd-app`)

| Variable | Descripción | Default típico |
|----------|-------------|----------------|
| `AUDIOSOCKET_ADDR` | `host:port` del gateway (ARI `external_host`) | `audiosocket:9092` |
| `AUDIOSOCKET_SESSION_TTL_SEC` | TTL de `OML:AUDIOSOCKET:{uuid}` | `3600` |

Definidas en `source/ari-app/config.py`. En Compose suelen mapearse desde `docker-compose/env`.

### 8.2 Agente voicebot (Redis / Django)

Por agente voicebot (campos típicos leídos en el loop):

- `VOICEBOT_TRANSPORT` = `audiosocket` \| `sip`
- `VOICEBOT_ADDR` / host SIP (solo SIP)
- Resto de flags voicebot existentes (`VOICEBOT=1`, límites de llamadas, etc.)

### 8.3 Servicio `audiosocket`

Ver tabla completa en el README del componente. Resumen:

| Variable | Uso |
|----------|-----|
| `AUDIOSOCKET_HOST` / `PORT` | Bind TCP (9092) |
| `VOICEBOT_VENDOR` | `ces` |
| `REDIS_*` | Lectura sesión + publish handoff |
| `APP_ID` / `PROJECT_ID` / `LOCATION` | CES (`APP_ID` obligatorio) |
| `GOOGLE_APPLICATION_CREDENTIALS` | SA GCP |
| `AUDIOSOCKET_GCP_SCOPE` | Default `cloud-platform`; opt-in `ces` |
| `OML_API_BASE_URL`, `OML_VOICEBOT_USERNAME` / `PASSWORD` | Webhook / new_contact |
| `AUDIOSOCKET_DEBUG_PII` | Logs de transcripts/teléfonos (off por defecto) |

Compose: servicio `audiosocket`, imagen `AUDIOSOCKET_IMG`, volume SA GCP.

---

## 9. Prompt CES / `end_session`

El agente CES debe **invocar** la tool de sistema `end_session` (no narrarla en texto hablado):

| Campo | Uso |
|-------|-----|
| `session_escalated` / `sessionEscalated` | `true` → webhook + `voicebot_handoff`; `false` → fin de llamada |
| `reason` | Log + campo en POST Django |
| `params.call_summary` / `Call_Summary` | Observaciones del agente humano |

Ejemplos:

- Acepta / escala: `end_session(reason="servicio_aceptado", session_escalated=True, params={"call_summary": "..."})` + frase de espera.
- Rechaza: `end_session(reason="servicio_rechazado", session_escalated=False)` + despedida.

El gateway incluye fallback si el modelo verbaliza `end_session(...)` en el transcript; lo ideal es el evento estructurado.

---

## 10. Observabilidad y troubleshooting

### Logs esperados (escalación OK)

**audiosocket:**

```text
Sesión iniciada con UUID de Asterisk: …
Sesión OML cargada: Contact=… Campaign=… Call=… phone=****NNNN
Conectado a Google CES: projects/…/apps/…/sessions/…
endSession reason='…' session_escalated=True
OML … OK …
voicebot_handoff publicado en acd:commands:global …
Llamada finalizada.
```

**acd-app:**

```text
CommandDispatcher: voicebot_handoff OK call_id=…
begin_voicebot_handoff: start_distribution call_id=… campaign_id=…
```

### Señales de problema

| Log / síntoma | Causa probable |
|---------------|----------------|
| `Agente: end_session(...)` / `print(default_api.end_session…)` sin `endSession reason=` | CES verbalizó la tool; revisar prompt / tool; el fallback debería disparar handoff tras el fix |
| `1011 resource_exhausted` | Cuota / rate limit Google CES; esperar y reintentar |
| `Healthcheck TCP (Docker): peer 127.0.0.1…` | Probe del `HEALTHCHECK` del contenedor; **no** es llamada |
| Hangup inmediato sin UUID Asterisk desde IP no local | Peer cerró TCP sin protocolo |
| `AUDIOSOCKET_ADDR vacío` | ACD no puede discar AudioSocket |
| Webhook skip / error + handoff igual | Faltan credenciales OML o GESTION_BOT; handoff sigue |

---

## 11. Seguridad operativa

- Contenedor `audiosocket`: usuario no-root, `HEALTHCHECK` TCP, deps pinneadas, secretos fuera del repo.
- PII en logs: truncado/máscara; detalle con `AUDIOSOCKET_DEBUG_PII=1` solo en troubleshooting.
- Redis de comandos no debe exponerse a proveedores externos; el gateway es el único publicador de `voicebot_handoff` desde el path CES.
- Credenciales: rotar SA GCP y password del agente API (`GESTION_BOT` / webhook).

Detalle ISO / CI: README de `audiosocket`.

---

## 12. Extensión / fuera de alcance actual

**Implementado**

- Dual SIP + AudioSocket por `VOICEBOT_TRANSPORT`
- Gateway CES + webhook Django + `voicebot_handoff`
- Handoff a cola/campaña humana (misma campaña del contexto)

**No implementado** (respecto a borradores anteriores)

- Acciones Redis granulares `bot_handoff_agent` / `bot_session_end` / `bot_fallback_human` desde el gateway
- `BotAdapter` / `Bot Session Manager` como capa abstracta formal
- SIP REFER desde el path AudioSocket
- Multi-vendor en producción más allá del contrato `VoicebotAdapter` + `VOICEBOT_VENDOR`

Para un vendor nuevo: implementar `adapters/<vendor>.py` en el repo `audiosocket` y registrar en `factory.py` (PCM 8 kHz + semántica `session_escalated`).

---

## 13. Referencias

- Asterisk ARI `externalMedia`: [Channels REST API](https://docs.asterisk.org/Latest_API/API_Documentation/Asterisk_REST_Interface/Channels_REST_API/)
- Protocolo AudioSocket: [AudioSocket](https://docs.asterisk.org/Configuration/Channel-Drivers/AudioSocket/)
- Componente gateway: [`../../audiosocket/README.md`](../../audiosocket/README.md)
- Código ACD clave:
  - `source/ari-app/services/call_manager.py` → `dial_voicebot_audiosocket`
  - `source/ari-app/services/command_dispatcher.py` → `voicebot_handoff`
  - `source/ari-app/services/distribution_service.py` → loop transport + `begin_voicebot_handoff`
  - `source/ari-app/sip_refer_listener.py` → path SIP
