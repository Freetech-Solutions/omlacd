# Sanity check Inbound

Checklist de regresión para flujo **Inbound** tras cambios de fairness ACD
(claim+reserve atómico, P0 release/`RINGING`/loop generation, HOL offering).

Cubre: señalización/funcionamiento, **Redis** (estado en vivo) y **Postgres**
vía call-logger → tablas actuales:

- `interactions_summary` (`InteractionsSummary`) — CDR / cierre de interacción
- `interaction_transfers` (`InteractionTransfers`) — transferencias

No usar el esquema legacy `reportes_app_llamadalog` / eventos tipo
`ENTERQUEUE` / `CONNECT` / `COMPLETE*` / `BT-*` / `CT-*` / `CAMPT-*`.

## Prerrequisitos

- Compose (`test-env` / `dev-env`) arriba; ACD + Django + Redis + Postgres + softphones.
- ≥2 agentes READY en la misma cola inbound.
- Ideal: segunda campaña inbound (blind-to-campaign / weight cross-cola).
- Anotar `interaction_id` (= uniqueid / callid de negocio) de cada prueba.

## Herramientas

Desde `docker-compose/<env>`:

```bash
./oml_manage.sh logs -f acd-app
./oml_manage.sh logs -f call-logger
./oml_manage.sh asterisk_cli      # core show channels / bridge show all
./oml_manage.sh sngrep
./oml_manage.sh psql omnileads    # ajustar user/DB del entorno
```

Redis (servicio/`DB` que use el ACD):

```bash
docker compose exec redis redis-cli
```

### Claves Redis

| Clave | Qué mirar |
|-------|-----------|
| `acd:queue:{camp}:waiting` | ZSET `call_id` → `enqueued_at_ms` |
| `acd:queue:waiting:alive:{call_id}` | Heartbeat TTL (`ACD_WAITING_ALIVE_TTL_SEC`, default 90s); sin él el head se purga |
| `OML:AGENT:{id}` | `STATUS`, `CALLID`, `TIMESTAMP` |
| `acd:lock:agent:{id}` | valor = `call_id` durante reserva/ring |
| `acd:lease:agent:{id}` | idem (lazy cleanup DIALING) |
| `acd:offer:best:{id}` | offer gate (weight) |

### SQL Postgres — InteractionsSummary

Una fila por interacción (cierre). `status` / `hangup_cause` usan causas ACD
(`HangupCause`), p. ej. `EXIT_ANSWERED`, `EXIT_ABANDON`, `EXIT_TIMEOUT`.

```sql
SELECT
  interaction_id,
  campaign_id,
  channel_type,
  direction,
  initiation_method,
  status,
  hangup_cause,
  agent_id,
  start_time,
  end_time,
  total_duration,
  wait_conn_duration,
  agent_duration,
  bot_duration,
  is_transferred,
  transfer_count
FROM interactions_summary
WHERE interaction_id = '<UNIQUEID>';
```

Inbound típico esperado en happy path:

| Campo | Valor esperado |
|-------|----------------|
| `direction` | `INBOUND` |
| `initiation_method` | `AGENT` (humano) o `BOT` (solo voicebot) |
| `status` | `EXIT_ANSWERED` (atendida) |
| `agent_id` | id del agente que atendió (`NULL`/negativo si abandon/timeout sin agente) |
| `agent_duration` | `> 0` si hubo conversación con agente |
| `wait_conn_duration` | ≥ 0 (espera en cola hasta conectar) |
| `is_transferred` | `false` si no hubo xfer; `true` si sí |
| `transfer_count` | `0` o `≥ 1` según xfers |

### Status / hangup relevantes (Inbound)

| Status | Cuándo |
|--------|--------|
| `EXIT_ANSWERED` | Atendida y finalizada con agente (o consolidada) |
| `EXIT_SHORTCALL` | Atendida pero corta (umbral shortcall; bridge ACD–agente) |
| `EXIT_ABANDON` | Cliente abandona en cola (sin atender) |
| `EXIT_TIMEOUT` | Timeout de cola |
| `EXIT_HANDOFF_ABANDON` | Tras handoff voicebot→humano, cliente abandona |
| `EXIT_HANDOFF_TIMEOUT` | Tras handoff, timeout de cola humana |

### SQL — InteractionTransfers

```sql
SELECT
  interaction_id,
  source_agent_id,
  destination_type,          -- AGENT | CAMPAIGN | EXTERNAL
  destination_id,
  destination_agent_id,
  destination_campaign_id,
  transfer_type,             -- BLIND | CONSULT | …
  status,                    -- OK | (fail)
  fail_reason,
  created_at,
  completed_at,
  talk_time_after
FROM interaction_transfers
WHERE interaction_id = '<UNIQUEID>'
ORDER BY created_at, id;
```

---

## A. Happy path (1 agente)

### Señalización / softphone

- [ ] PSTN entra → MOH; softphone recibe invite con `X-OML-Origin: IN` (no MANUAL).
- [ ] Ring → answer → audio PSTN↔agente; MOH corta.
- [ ] Hangup agente o PSTN → canales/bridge limpios.

### Redis — durante ring

- [ ] Tras claim: call sale del ZSET waiting (`distribution_offering`); `STATUS=DIALING` (luego puede pasar a `RINGING` vía Django).
- [ ] `acd:lock:agent:{id}` y `acd:lease:agent:{id}` = ese `call_id`.

### Redis — tras ONCALL

- [ ] `STATUS=ONCALL`, `CALLID` correcto.
- [ ] Lock / lease / offer **ausentes**.
- [ ] Call **no** está en `acd:queue:{camp}:waiting`.

### Redis — tras hangup

- [ ] Agente `READY` (o POSTCALL según regla).
- [ ] Sin lock/lease/offer huérfanos; ZSET sin el `call_id`.

### Postgres (`interactions_summary`)

- [ ] Una fila con `interaction_id` = callid.
- [ ] `status = EXIT_ANSWERED` (happy path normal).
- [ ] `direction = INBOUND`, `initiation_method = AGENT`.
- [ ] `campaign_id` / `agent_id` correctos.
- [ ] `agent_duration > 0`, `total_duration > 0`, `wait_conn_duration` coherente.
- [ ] `is_transferred = false`, `transfer_count = 0`.
- [ ] Sin fila en `interaction_transfers` para este id.

### Logs ACD

- [ ] `Loop iniciado` → claim/reserve → answer accepted → `try_confirm_distribution_oncall` OK → `finalize_waiting_after_oncall` → loop finalized (gen actual).

---

## B. Fairness / HOL (2+ agentes, 2+ llamadas misma cola)

| ID | Caso | Señalización | Redis | Postgres (`interactions_summary`) | OK |
|----|------|--------------|-------|-----------------------------------|----|
| B1 | A ringing, B entra, 2º agente READY | B origina al 2º **sin** esperar ONCALL de A | A fuera ZSET (offering); B head u offering; 2 locks | 2 filas `EXIT_ANSWERED` (al colgar cada una), `agent_id` distintos | [ ] |
| B2 | 1 solo READY; A suena; B espera | B **no** roba agente de A | Un lock/lease; offer no pisa | B no cierra como atendida hasta que A libere o B abandone/timeout | [ ] |
| B3 | A no contesta (ring timeout) | Siguiente candidato | Agente → READY, sin lock; no DIALING/RINGING huérfano | Sin `EXIT_ANSWERED` fantasma por el intento fallido | [ ] |
| B4 | Abandono PSTN en cola | MOH corta; cleanup | ZSET limpio; agentes READY | `status = EXIT_ABANDON` | [ ] |
| B5 | Timeout de cola | Hangup PSTN | Cleanup | `status = EXIT_TIMEOUT` | [ ] |
| B6 | Reinicio acd-app con C1 zombie en ZSET; entra C2 + agente READY | C2 debe poder originar (purge al boot o ≤ `ACD_WAITING_ALIVE_TTL_SEC`) | C1 sin alive / purgado; C2 head u offering | C2 puede cerrar `EXIT_ANSWERED`; no quedar bloqueado por C1 muerto | [ ] |

---

## C. Bridge fail / redistribute (post-answer)

Si se puede forzar fallo de bridge / ONCALL post-answer (o hangup del leg agente justo tras answer antes de consolidar):

- [ ] PSTN sigue en cola/MOH; **no** se inserta `EXIT_ANSWERED` prematuro.
- [ ] `distribution_offering` limpio; re-enqueue ZSET con **mismo score** (FIFO).
- [ ] Agente intentado → READY (release DIALING\|RINGING).
- [ ] Nuevo loop (gen nueva); finally del loop viejo **no** cuelga el intento nuevo (log: skip cleanup obsoleto).
- [ ] Consolidación OK → ONCALL + finalize; al colgar: `status = EXIT_ANSWERED` con `agent_duration` coherente.

---

## D. Softphone RINGING → ONCALL

- [ ] Answer con modal ringing (`DIALING`→`RINGING` vía Django).
- [ ] ACD confirma ONCALL (no rechaza por STATUS).
- [ ] Redis: `ONCALL` + sin lock/lease.
- [ ] Cancel/abandon mid-RINGING → release → READY.
- [ ] Si llega a conversación: `EXIT_ANSWERED`; si abandona en ring sin consolidar: no inventar answered.

---

## E. Transferencias (inbound ya consolidado)

Anotar `interaction_id` y consultar **ambas** tablas.

### E1. Blind a agente

| Fase | Señalización | Redis | Postgres | OK |
|------|--------------|-------|----------|----|
| TRY | Originate destino | Destino DIALING/RINGING/ONCALL | Aún puede no haber fila transfer hasta resultado | [ ] |
| OK | Audio con destino | Initiator limpio; destino ONCALL | `interaction_transfers`: `transfer_type=BLIND`, `destination_type=AGENT`, `destination_agent_id` set, `status=OK`; summary: `is_transferred=true`, `transfer_count≥1`, cierre típ. `EXIT_ANSWERED` | [ ] |
| FAIL | Busy/noanswer según diseño | Sin locks huérfanos; READY | Transfer `status≠OK` + `fail_reason` si aplica; summary sin answered fantasma del destino | [ ] |

### E2. Blind a campaña

- [ ] Sale de agente A; entra cola destino (MOH); redistribución.
- [ ] Redis: ZSET waiting de **camp destino**; agente A READY.
- [ ] `interaction_transfers`: `transfer_type=BLIND`, `destination_type=CAMPAIGN`, `destination_campaign_id` = camp destino, `status=OK`.
- [ ] `interactions_summary`: `is_transferred=true`, `transfer_count≥1`; `status` final según destino (p. ej. `EXIT_ANSWERED` / `EXIT_TIMEOUT` / `EXIT_ABANDON` en el cierre de la interacción).
- [ ] HOL en cola destino si hay 2 llamadas transferidas (igual criterio B1).

### E3. Consultativa

| Paso | Señalización | Redis | Postgres | OK |
|------|--------------|-------|----------|----|
| `consult_start` | Pierna Up; A↔consultado; PSTN hold | Consultation activa | Puede no insertar transfer hasta complete/fail | [ ] |
| `consult_complete` | A sale; PSTN+consultado | A READY; consultado ONCALL | `transfer_type=CONSULT`, `destination_type=AGENT` (o CAMPAIGN si aplica), `status=OK`; summary `is_transferred=true` | [ ] |
| `consult_cancel` | Vuelve A+PSTN | Sin piernas huérfanas | Sin transfer OK espurio; summary cierra como inbound normal (`EXIT_ANSWERED` si A sigue) | [ ] |

### E4. Hangups mid-xfer

- [ ] Hangup PSTN mid-blind/consult → cleanup + READY; transfer fail o summary coherente (no `EXIT_ANSWERED` inconsistente).
- [ ] Hangup destino mid-ring → initiator no queda en DIALING; transfer `status≠OK` si corresponde.

---

## F. Multi-cola / weight (`ACD_QUEUE_WEIGHT_ENABLED=true`)

- [ ] Dos campañas con distinto weight; agente(s) en ambas o pools solapados.
- [ ] Idle en ambas: gana el head con mayor CallPriority (weight+age).
- [ ] Tras offering, la otra cola puede competir por otros READY.
- [ ] Un solo ganador por agente (`acd:offer:best:{id}` coherente).
- [ ] Al cerrar: cada `interactions_summary.campaign_id` refleja la cola efectiva.

---

## G. Caches / supervisión

Tras ONCALL y tras cierre (`EXIT_*`):

- [ ] `OML:AGENT:{id}` alineado con UI supervisión (READY/ONCALL).
- [ ] Si aplica: `OML:AGENTDATA:AGENT:{id}` (answered IN) sin corrupción.
- [ ] Sin “fantasma” ONCALL en wallboard tras hangup/xfer.

---

## H. Criterios de fallo rápido

Marcar **FAIL** de la corrida si aparece cualquiera:

1. Agente en `DIALING`/`RINGING` > ring_timeout+margen sin lock/lease y sin llamada real.
2. Lock/lease con `call_id` muerto tras abandon/timeout/xfer.
3. Call en ZSET waiting **después** de ONCALL estable.
4. 2ª llamada misma cola no suena con 2º agente libre mientras la 1ª ringea (HOL).
5. Tras reinicio/crash, un `call_id` muerto en el ZSET bloquea a los demás más de `ACD_WAITING_ALIVE_TTL_SEC` (o no se limpia al boot si era local).
6. Softphone inbound con Origin MANUAL / sin auto-attend esperado.
7. Answer con `STATUS=RINGING` rechazado (no pasa a ONCALL).
8. Tras redistribute, finally viejo cuelga el nuevo intento.
9. Happy path sin fila en `interactions_summary`, o `status` distinto de `EXIT_ANSWERED` sin motivo.
10. Abandon/timeout reportados como `EXIT_ANSWERED`.
11. Blind/consult con audio OK pero sin fila `interaction_transfers` (`status=OK`) o summary con `is_transferred=false`.

### Barrido final Redis

```text
KEYS acd:lock:agent:*
KEYS acd:lease:agent:*
KEYS acd:offer:best:*
KEYS acd:queue:waiting:alive:*
ZRANGE acd:queue:{camp}:waiting 0 -1 WITHSCORES
```

- [ ] Solo claves de llamadas **vivas**; resto vacío / agentes READY.

---

## Orden sugerido (~30–45 min)

1. A — happy path + SQL summary + snapshot Redis  
2. B1 + B2 — HOL + no-steal  
3. B3 ring timeout + B4 abandon (`EXIT_ABANDON`) + B5 (`EXIT_TIMEOUT`)  
4. D — RINGING→ONCALL  
5. E1 blind agent OK + FAIL (`interaction_transfers`)  
6. E2 blind campaign  
7. E3 consult complete + cancel  
8. F weight (si aplica)  
9. Barrido final H  

## Referencias de código

- Handler: `source/ari-app/handlers/inbound.py`
- Distribución / HOL / P0: `source/ari-app/services/distribution_service.py`
- Reserva / ONCALL: `source/ari-app/services/agent_status_service.py`
- Waiting / offer: `source/ari-app/services/routing/`
- Transferencias: `source/ari-app/transfer.py`, `services/command_dispatcher.py`
- Causas: `source/ari-app/constants.py` (`HangupCause`)
- Persistencia: `source/workers/logger.py` → `interactions_summary` / `interaction_transfers`
- ORM: `django/reportes_app/models.py` (`InteractionsSummary`, `InteractionTransfers`)
