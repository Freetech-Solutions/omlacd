# Analisis tecnico ACD - 15 abr

Analisis realizado sobre el codigo actual de `components-git-repo/acd/source/ari-app` y tests asociados en `components-git-repo/acd/source/tests_unit`.

## A. Resumen ejecutivo de la aplicacion

La aplicacion ACD es un runtime ARI orientado a eventos que mantiene el estado de cada llamada en Redis mediante `state.CallRegistry`, expone operaciones de marcado y manipulacion via Gearman y Redis Pub/Sub, y usa `router.AcDRouter` como punto de convergencia de los eventos ARI.

La arquitectura tiene cuatro planos claramente diferenciados:

1. Plano de entrada:
   - WebSocket ARI en `main.py` y `router.py`.
   - Tareas de inicio de llamada por Gearman en `infrastructure/gearman_listener.py`.
   - Comandos operativos por Redis Pub/Sub en `infrastructure/command_listener.py`.
   - SIP REFER para voicebot en `sip_refer_listener.py`.
2. Plano de orquestacion:
   - `handlers/manual.py`, `handlers/inbound.py` y `handlers/campaign.py`.
   - `transfer.py` para transferencias y handoff especiales.
   - `services/distribution_service.py` para cola, timeouts y distribucion.
3. Plano de estado:
   - `state.CallContext`, `state.ConsultationData`, `state.CallRegistry`.
   - Helpers de coherencia en `state_helpers.py`.
4. Plano de salida:
   - `reporter.ACDReporter` para eventos hacia `acd-log-processor`.
   - `queue_events.QueueEventManager` para tamanio de cola y `OML:CHANNEL:CALLEVENTS`.
   - `services/agent_status_service.py` para `OML:AGENT:{id}`.

La fuente de verdad logica es Redis, no ARI. ARI representa el estado fisico de canales, bridges, MOH y grabaciones. La aplicacion pasa continuamente por ventanas donde ambos planos divergen a proposito, sobre todo durante transferencias.

## B. Arquitectura general

### B.1 Bootstrap y ensamblado

`containers.ACDContainer` arma el grafo principal:

- `ari_manager.ARI`: wrapper HTTP de ARI con operaciones de alto nivel como `originate_channel_op`, `add_channel_to_bridge`, `hangup_channel`, `destroy_bridge`, `get_channel_details`.
- `state.CallRegistry`: persistencia de `CallContext` en Redis, con indices secundarios por canal y bridge.
- `services.call_manager.CallActionService`: envoltorio de operaciones de marcado y bridge.
- `services.queue_strategy.QueueStrategyEngine`: seleccion de candidatos.
- `services.distribution_service.DistributionService`: loop de distribucion, timer de cola y reserva temporal de agentes.
- `handlers.manual.ManualCallHandler`, `handlers.inbound.InboundCallHandler`, `handlers.campaign.ProgressiveCampaignHandler`.
- `transfer.TransferManager`.
- `router.AcDRouter`.
- `services.command_dispatcher.CommandDispatcher`.
- `infrastructure.command_listener.CommandListener`.
- `infrastructure.gearman_listener.GearmanListener`.

`main.py` levanta el contenedor, el WebSocket de ARI y los listeners auxiliares. La aplicacion es intensamente multihilo: WebSocket ARI, timers de cola, loops de distribucion, hilos de REFER voicebot y listeners de comandos.

### B.2 Responsabilidad por componente

`router.AcDRouter`

- Parseo de eventos ARI.
- Resolucion del contexto de llamada.
- Despacho a handlers por tipo de llamada.
- Logica transversal en `ChannelStateChange`, `ChannelDestroyed`, `ChannelHangupRequest`, `StasisEnd`.
- Integracion con `TransferManager`, `RecordingEventHandler`, `QueueEventManager`, `LegacyEventForwarder` y SIP REFER.

`handlers/inbound.py`

- Flujo inbound ACD clasico.
- Crea bridge, contesta PSTN, agrega PSTN al bridge, inicia MOH, registra contexto y arranca distribucion.
- Consolida al agente cuando su canal entra a Stasis.
- Maneja abandono, timeout, answer, hangup del agente y cleanup final.

`handlers/campaign.py`

- Flujo progressive/dialer humano.
- Tiene la misma segunda mitad de inbound, pero la primera parte viene del dialer y puede pasar por AMD.
- Usa `pending_amd` en Redis y tiene una rama especial para handoff desde voicebot.

`handlers/manual.py`

- Flujo click-to-call/manual.
- El agente entra primero, se crea bridge, luego se origina PSTN.
- Tiene su propio cierre de llamada y es el unico flujo donde `TransferManager.on_transfer_target_hangup()` aplica una politica especial explicita.

`services/distribution_service.py`

- Tiene dos loops: `_run_distribution_loop()` y `_run_voicebot_distribution_loop()`.
- Mantiene por llamada:
  - `stop_event`
  - `attempt_finished`
  - timer de cola
  - canal en intento (`_active_attempts`)
  - agente en intento (`_active_attempt_agents`)
- Dispara `_on_queue_timeout()` cuando vence el tiempo maximo en cola.

`services/agent_status_service.py`

- Actualiza `OML:AGENT:{id}`.
- Principalmente cambia `STATUS`, `CALLID`, `BRIDGE_ID`, `CAMPAIGN`, `CONTACT_NUMBER`.
- Se usa en answer real, no como reserva preventiva de intento.

`services/queue_strategy.py`

- Lee `OML:AGENT:{id}` por pipeline.
- Filtra candidatos humanos por `STATUS == READY`.
- Ordena por penalidad y estrategia.
- Para voicebot no exige `READY`; filtra por `VOICEBOT=1`.

`state.py`

- `CallContext` modela el estado logico.
- `ConsultationData` encapsula la pierna consultiva y el bridge temporal.
- `CallRegistry` persiste JSON con indices `acd:{node}:idx:channel:{channel}` y `acd:{node}:idx:bridge:{bridge}`.
- `mark_call_ended_atomic()` usa `SETNX` sobre `acd:{node}:call:{call_id}:call_ended` para deduplicar finales.

`reporter.py`

- Envia `DIAL`, `ANSWER`, `segment_end` y `TRANSFER` por Gearman a `acd-log-processor`.
- `id_camp` se usa como campania de atribucion/reporting.

`queue_events.py`

- Mantiene `OML:CALLDATA:QUEUE-SIZE:{camp}` y `OML:CALLDATA:QUEUE:{camp}`.
- Publica eventos al canal Redis `OML:CHANNEL:CALLEVENTS`.
- Conserva cache en memoria y sincroniza contra Redis cuando detecta inconsistencia.

### B.3 Flujo de eventos global

Secuencia general:

1. Una llamada o comando entra por Gearman, Redis Pub/Sub o ARI.
2. Se origina o se recibe un canal en Stasis.
3. El handler correspondiente crea o actualiza `CallContext`.
4. Operaciones ARI materializan el estado fisico.
5. `ChannelStateChange`, `ChannelDestroyed`, `ChannelHangupRequest` y `StasisEnd` van cerrando las transiciones.
6. `reporter` y `queue_event_manager` emiten la vista de negocio.
7. `CallRegistry.unregister()` elimina el estado cuando la llamada termina.

## C. Modelo de estado y persistencia

### C.1 `CallContext`

Campos clave y semantica real:

- `call_id`: identidad logica de negocio. Es la llave principal del `CallRegistry`. En transferencias se intenta seguir usando este id, no los channel ids.
- `bridge_id`: bridge principal de la llamada. En consultiva no cambia; el bridge temporal vive en `consultation.consult_bridge`.
- `pstn_channel` / `uniqueid_pstn`: referencia fisica y tecnica al leg cliente/PSTN.
- `agent_attempt_channel`: pierna de agente originada pero aun no consolidada.
- `agent_connected_channel`: pierna de agente actualmente consolidada en el bridge principal.
- `agent_id`: agente actual logico. Puede apuntar al agente B despues de una transferencia aunque `uniqueid_agent` siga apuntando a A.
- `target_agent_id`: destino deseado de una blind transfer antes de consolidarla.
- `consultation`: bloque consultivo temporal.
- `transfer_in_progress`: cerrojo logico para bloquear operaciones mientras una transferencia esta en ventana critica.
- `is_transferred`: se vuelve `True` solo cuando una transferencia se considera finalizada en terminos de negocio.
- `transfer_count`: contador acumulado de transferencias finalizadas.
- `distribution_campaign_id`: campania operativa actual de cola. Se usa tras `blind_to_campaign`.
- `id_camp`: campania original de atribucion. No debe sobreescribirse al re-encolar.
- `call_ended`: flag logico final, deduplicado en Redis por `mark_call_ended_atomic()`.
- `ignore_next_agent_hangup`: proteccion para ignorar el hangup del agente iniciador despues de `consult_complete`.
- `agent_answered_ts`: primer timestamp de answer del agente actual; `finalize_current_agent_segment()` lo usa para cerrar segmentos.
- `other_channels`: canales auxiliares, por ejemplo `three_way_conf`.
- `custom_sip_headers`: headers transportados, sobre todo en `blind_to_campaign`.

Campos auxiliares importantes en transferencias:

- `transfer_phase`: `none`, `requested`, `answered`, `finalized` a nivel semantico, no fisico.
- `blind_transfer_leg_id`
- `blind_transfer_report_state`
- `blind_transfer_pending_up_report`
- `blind_transfer_numero_extra`
- `blind_transfer_agente_origen_id`
- `blind_transfer_initiated_by`
- `voicebot_transfer_waiting`
- `voicebot_leg_start_ts`
- `voicebot_leg_end_ts`
- `is_voicebot_transfer`
- `agent_segments`

### C.2 `ConsultationData`

`ConsultationData` modela la transferencia consultiva:

- `active`: la consulta sigue vigente.
- `initiator_agent_ch`: canal del agente A.
- `main_bridge`: bridge principal.
- `target_agent_id` / `target_endpoint`: destino logico.
- `consult_bridge`: bridge temporal de consulta.
- `consult_leg_ch`: canal del agente B o endpoint consultado.
- `consult_leg_answered_ts`: evidencia de que B realmente quedo `Up`.
- `target_agent_uniqueid`: declarado pero no usado de forma relevante en el flujo actual.

### C.3 `CallRegistry`

Persistencia real:

- Clave principal: `acd:{node_id}:call:{call_id}`.
- Lock distribuido: `acd:{node_id}:lock:{call_id}`.
- Indice por canal: `acd:{node_id}:idx:channel:{channel_id}`.
- Indice por bridge: `acd:{node_id}:idx:bridge:{bridge_id}`.

Propiedades importantes:

- `register()` adquiere lock.
- `register_unsafe()` exige que el lock ya este tomado.
- `get_by_channel()` y `get_by_bridge_id()` usan indices secundarios.
- `unregister()` borra contexto, indices y el flag `call_ended`.
- `_get_all_associated_channels()` indexa:
  - PSTN
  - attempt channel
  - connected channel
  - `uniqueid_agent`
  - `uniqueid_pstn`
  - consult leg
  - initiator consultivo
  - snoop channels
  - other channels
  - `blind_transfer_leg_id`

### C.4 Helpers de estado

Helpers centrales en `state_helpers.py`:

- `active_agent_channel(ctx)`: devuelve solo la pierna consolidada.
- `effective_queue_campaign_id(ctx)`: usa `distribution_campaign_id` si existe; si no, `id_camp`.
- `call_transfer_routing_active(ctx)`: `True` si `is_transferred`, `transfer_in_progress` o blind reporting sigue en `requested`.
- `call_has_prior_agent_handling(ctx)`: detecta si ya hubo agente, incluso si hoy no hay uno conectado.
- `finalize_current_agent_segment(ctx)`: cierra el tramo actual de agente y lo agrega a `agent_segments`.
- `queue_timeout_should_suppress_cleanup(ctx)`: suprime timeout solo si hay agente conectado y `agent_answered_ts`.

### C.5 Estado logico vs estado fisico

Estado logico en Redis:

- Que agente deberia ser el actual.
- Que transferencia esta en curso.
- Que campania opera.
- Que tramo de conversacion ya se cerro.

Estado fisico en ARI:

- Si el canal existe.
- Si esta `Up`, `Ringing`, `Down`.
- Si pertenece o no a un bridge.
- Si hay MOH activa.

Divergencias deliberadas:

- `blind_to_endpoint()` cuelga al agente A antes de originar B, pero deja `agent_connected_channel` apuntando a A hasta que `on_transfer_leg_start()` consolida o falla.
- `consult_start()` mueve fisicamente al agente A al consult bridge mientras el contexto principal sigue apuntando al main bridge.
- `blind_to_campaign()` deja `transfer_in_progress=True` y `ignore_next_agent_hangup=True` antes de colgar al agente para que un `ChannelHangupRequest` temprano no cierre la llamada.

## D. Flujo completo de llamada inbound

### D.1 Entrada a Stasis y setup inicial

`InboundCallHandler.on_start()` hace, en este orden:

1. Extrae `id_camp`, `id_customer`, `tel_customer`, `tel_dialed`, `callid`.
2. Carga configuracion de campania desde Redis con `_load_campaign_cfg()`.
3. Determina `call_id` y `uniqueid`.
4. Crea bridge via `CallActionService.create_bridge()`.
5. Contesta el canal PSTN con `ari.answer(channel_id)`.
6. Agrega el PSTN al bridge con `call_service.add_channel_to_bridge()`.
7. Inicia MOH sobre el bridge.
8. Crea `CallContext` via `_create_and_register_context()`.
9. Reporta entrada a cola con `reporter.log_queue()`.
10. Notifica `QueueEventManager.on_enter_queue()`.
11. Inicia `DistributionService.start_distribution()` o `start_voicebot_distribution()`.

### D.2 Creacion del bridge

El bridge principal es persistido en `ctx.bridge_id`. No existe un bridge secundario salvo en consultiva. La llamada inbound vive desde el inicio dentro de un bridge mixing aunque aun no haya agente.

### D.3 Answer del canal PSTN

El answer del PSTN es inmediato en `on_start()`, antes de comenzar la distribucion. Esto hace que el cliente escuche MOH desde el bridge ACD y no desde el dialplan.

### D.4 AddChannel al bridge

El PSTN entra primero al bridge. El agente entra mas tarde cuando su `channel_type=to_agent` llega a `StasisStart` y `InboundCallHandler.on_agent_stasis_start()`:

1. Llama `distribution_service.handle_agent_answer(call_id, channel_id)`.
2. Detiene el loop y cancela timer de cola.
3. Agrega el canal del agente al bridge.
4. Promueve `agent_attempt_channel -> agent_connected_channel`.
5. Marca `agent_answered_ts`.
6. Actualiza `agent_id`.
7. Actualiza estado de agente a `ONCALL`.
8. Detiene MOH.
9. Notifica `QueueEventManager.on_answered()`.

### D.5 MOH

Se activa:

- En el inicio inbound al entrar a cola.
- En `blind_to_endpoint()`.
- En `blind_to_campaign()`.
- En `consult_start()` sobre el main bridge mientras A habla con B en el consult bridge.
- En `on_channel_hold` si el agente hace hold.

Se detiene:

- Cuando un agente humano queda efectivamente conectado.
- En `consult_complete()` y `consult_cancel()`.
- En ciertos caminos de recuperacion tras fallos.

### D.6 Entrada y salida de cola

Entrada:

- `reporter.log_queue()`.
- `QueueEventManager.on_enter_queue()`.

Salida:

- `on_answered()` si contesta agente.
- `on_timeout()` si vence el timer.
- `on_abandon()` si cuelga el cliente antes.

### D.7 Distribucion a agentes

Se delega completamente a `DistributionService`. El handler solo:

- inicializa el loop,
- interpreta el `StasisStart` del agente contestado,
- y ejecuta el cleanup final si el PSTN se va.

### D.8 Conexion con agente

La conexion no se decide por `ChannelStateChange Up`, sino por `StasisStart` del leg to_agent. `ChannelStateChange Up` se usa mas como confirmacion secundaria y para transferencias.

### D.9 Grabacion

`RecordingEventHandler.handle_channel_entered_bridge()` arranca grabacion cuando:

- `RecordingService.should_start_recording()` detecta al menos dos canales relevantes.
- En inbound, el trigger real es la entrada del canal de agente al bridge.

La grabacion es por bridge. `RecordingFinished` busca el contexto por `bridge_id` o `recording_id` y deriva el archivo a `RecordingManager`.

### D.10 Finalizacion y cleanup

Cuando el PSTN sale de Stasis, `InboundCallHandler.on_pstn_stasis_end()`:

1. Verifica que el canal sea el PSTN real.
2. Destruye bridge remanente.
3. Cuelga cualquier leg de agente remanente.
4. Detiene distribucion.
5. Ejecuta `mark_call_ended_atomic()`.
6. Decide `EXIT_ANSWERED`, `EXIT_TIMEOUT` o `EXIT_ABANDON`.
7. Reporta a `reporter`.
8. Notifica `QueueEventManager`.
9. Hace `CallRegistry.unregister()`.

## E. Motor de distribucion

### E.1 Como se eligen candidatos

`DistributionService._run_distribution_loop()`:

1. Lee miembros de `OML:CAMPAIGN-AGENTS:{id_camp}`.
2. Usa cache local temporal de miembros.
3. `QueueStrategyEngine.get_candidates()` trae `OML:AGENT:{id}` por pipeline.
4. Filtra solo `STATUS=READY`.
5. Agrupa por `penalty`.
6. Ordena dentro de cada grupo con estrategia:
   - `ringall`
   - `leastrecent`
   - `fewestcalls`
   - `random`
   - `rrmemory`

Voicebot:

- `get_voicebot_candidates()` filtra por `VOICEBOT=1`.
- No exige `READY`.

### E.2 Datos usados por `DistributionService`

Inputs principales:

- `call_id`
- `campaign_id`
- `bridge_id`
- `strategy`
- `ring_timeout`
- `queue_timeout_sec`
- `pstn_channel_id`
- `uniqueid`
- `distribution_metadata`

`distribution_metadata` viaja a `dial_agent_with_headers()` o `dial_voicebot_with_headers()` y tipicamente contiene:

- `id_customer`
- `id_camp`
- `tel_customer`
- `callid`
- `call_type`
- `agent_id` del candidato actual

### E.3 Interaccion con `AgentStatusService`

`DistributionService` no marca `RINGING` al agente candidato. Solo:

- reserva temporalmente con `acd:lock:agent:{agent_id}` via Redis `SET NX EX 15`,
- y cuando el agente realmente contesta, el handler correspondiente usa `AgentStatusService.set_oncall()`.

Implicancias:

- El criterio de elegibilidad sigue siendo `STATUS=READY`.
- La exclusion de concurrencia durante el ringing depende mas del lock temporal que del estado del agente.

### E.4 Manejo de queue timeouts

`start_distribution()` y `start_voicebot_distribution()` crean un `threading.Timer`.

Al vencer, `_on_queue_timeout()`:

1. Ejecuta callback opcional para marcar que el hangup PSTN fue iniciado por la app.
2. Hace `stop_event.set()` y `attempt_finished.set()`.
3. Relee el contexto bajo lock.
4. Si `queue_timeout_should_suppress_cleanup()` es `True`, no limpia.
5. Marca `call_ended`.
6. Notifica `QueueEventManager.on_timeout()`.
7. Reporta `EXIT_TIMEOUT` o `EXIT_HANDOFF_TIMEOUT`.
8. Cuelga intento de agente si lo hay.
9. Cuelga PSTN.
10. Destruye bridge.
11. Hace `unregister()`.

### E.5 Que evita redistribuir una llamada ya conectada

Guardas principales:

- `call_ended=True`
- `active_agent_channel(context)` ya presente
- en timeout, `queue_timeout_should_suppress_cleanup()` exige tambien `agent_answered_ts`
- `handle_agent_answer()` toma la propiedad del canal en intento y dispara `stop_event`

### E.6 Campania de origen vs campania efectiva de distribucion

Distincion real del codigo:

- `id_camp`: campania de atribucion y reporte. Permanece estable.
- `distribution_campaign_id`: campania efectiva de cola despues de `blind_to_campaign`.
- `effective_queue_campaign_id(ctx)`: helper que define que campania se usa para:
  - `QueueEventManager`
  - `set_oncall(campaign_id=...)`
  - `VOICEBOT-CALLS`
  - redistribucion siguiente

`reporter.log_segment_end()` sigue enviando `id_camp` como campania de negocio original. Ese desacople es deliberado.

## F. Manejo de transferencias

## F.1 Principios generales

Todas las transferencias dependen de tres capas:

1. Estado logico bajo lock distribuido.
2. Operaciones fisicas ARI fuera del lock.
3. Revalidacion posterior antes de consolidar.

Flags y estructuras relevantes:

- `transfer_in_progress`
- `transfer_phase`
- `is_transferred`
- `transfer_count`
- `target_agent_id`
- `consultation`
- `ignore_next_agent_hangup`
- campos `blind_transfer_*`

### F.2 `blind_to_agent()`

Objetivo funcional:

- Resolver SIP de un agente y reutilizar la mecanica de `blind_to_endpoint()`.

Precondiciones:

- `AgentStatusService` disponible.
- `get_sip(target_agent_id)` devuelve interfaz valida.

Cambios de estado:

- No toca Redis directamente; delega todo a `blind_to_endpoint()`.

ARI:

- Ninguna operacion directa; todo ocurre dentro de `blind_to_endpoint()`.

Reporte:

- `TRANSFER_REQUESTED` inmediato y `OK` o `FAILED` diferido.

Rollback:

- Idem `blind_to_endpoint()`.

### F.3 `blind_to_endpoint()`

Objetivo funcional:

- Sacar inmediatamente al agente actual y transferir la llamada a un endpoint externo o a otro agente.

Precondiciones:

- Contexto existente.
- `transfer_in_progress=False`.
- Bridge principal existente.

Cambios de estado en Redis:

Primer lock:

- valida contexto,
- opcionalmente fuerza `ctx.agent_id` si faltaba,
- `transfer_in_progress=True`,
- opcional `target_agent_id`.

Segundo lock antes del originate:

- revalida que la transferencia siga activa,
- copia valores estables del contexto.

Lock posterior al originate:

- `blind_transfer_leg_id=leg_id`
- `blind_transfer_report_state="requested"`
- `blind_transfer_pending_up_report=False`
- `blind_transfer_numero_extra`
- `blind_transfer_agente_origen_id`
- `blind_transfer_initiated_by`
- `transfer_phase="requested"`

Locks distribuidos:

- El lock del `call_id` protege solo la parte logica.
- El originate y el hangup del agente salen del lock.

Operaciones ARI:

1. `POST /bridges/{bridge}/moh`
2. `hangup_channel(agent_channel)`
3. `originate_channel_op(... transfer_target:true, customer_id:{call_id})`

Canales y bridges:

- Bridge principal: se mantiene.
- Canal agente A: se cuelga antes del originate.
- Nuevo leg B: nace fuera del bridge y luego sera absorbido por `on_transfer_leg_start()`.

Flags afectados:

- `transfer_in_progress=True` hasta consolidacion.
- `target_agent_id` puede quedar seteado.
- `blind_transfer_*` queda armado para reporting bifasico.

Como evita cerrar la llamada cuando cuelga el agente transferente:

- No se basa en `ignore_next_agent_hangup`.
- La proteccion es mas bien global: `transfer_in_progress=True` hace que varios handlers ignoren cleanup destructivo mientras la transferencia esta abierta.

Que reporta y cuando:

- Inmediatamente: `resultado="TRANSFER_REQUESTED"`.
- Mas tarde:
  - `OK` en `on_transfer_leg_start()` si el leg ya esta `Up`, o en `try_finalize_blind_transfer_on_destination_up()`.
  - `FAILED` en `_on_blind_transfer_leg_destroyed_locked()` o `_report_blind_transfer_bridge_failed()`.

Rollback en errores:

- Si el originate lanza excepcion, solo limpia `transfer_in_progress` y reporta `FAILED`.
- Si la pierna muere luego del originate, `_on_blind_transfer_leg_destroyed_locked()` limpia blind tracking, baja `transfer_in_progress` y borra referencias de agente muertas.

Observacion importante:

- Esta transferencia es realmente ciega: una vez colgado A no existe una reversion funcional al agente original.

### F.4 `blind_to_campaign()`

Objetivo funcional:

- Sacar al agente actual y re-encolar al cliente en otra campania ACD.

Precondiciones:

- Contexto existente.
- `transfer_in_progress=False`.

Cambios de estado en Redis:

Primer lock:

- `transfer_in_progress=True`
- `transfer_phase="requested"`
- `custom_sip_headers` si llegaron
- `ignore_next_agent_hangup=True`

Segundo lock luego del hangup:

- `finalize_current_agent_segment(ctx)`
- `distribution_campaign_id=target`
- `agent_id=None`
- `agent_attempt_channel=None`
- `agent_connected_channel=None`
- `transfer_in_progress=False`
- `transfer_count += 1`
- `is_transferred=True`
- `transfer_phase="none"`

Locks distribuidos:

- El contexto se congela antes y despues de las operaciones fisicas.
- La redistribucion posterior ya no ocurre bajo el mismo lock.

Operaciones ARI:

1. MOH en el bridge principal.
2. `hangup_channel(old_agent_ch)`.
3. Luego `DistributionService.stop_distribution(... hangup_agent_channel=False)`.
4. Arranca una nueva distribucion hacia la campania destino.

Canales y bridges:

- El PSTN y el bridge principal permanecen.
- Sale el agente actual.
- La llamada vuelve a estado "en cola" dentro del mismo bridge.

Flags afectados:

- `ignore_next_agent_hangup` protege el hangup del agente A.
- `distribution_campaign_id` redefine la cola operativa.

Como evita cerrar la llamada cuando cuelga el agente transferente:

- Persistiendo `ignore_next_agent_hangup=True` antes del `hangup_channel`.
- `InboundCallHandler.on_hangup_request()` consume ese flag antes de aplicar cleanup.

Que reporta y cuando:

- Reporta `TRANSFER OK` inmediatamente, porque no hay una pierna destino a esperar.
- `QueueEventManager.on_enter_queue()` se invoca de nuevo para la campania destino.

Rollback:

- No hay rollback semantico al agente anterior.
- Si la redistribucion falla despues del reporte OK, el codigo cuelga PSTN y destruye bridge, pero no revierte el estado ni corrige el reporte.

### F.5 `consult_start()`

Objetivo funcional:

- Crear una llamada consultiva: sacar al agente A del main bridge, poner al cliente en MOH y establecer una conversacion temporal A-B.

Precondiciones:

- Contexto existente.
- `ctx.bridge_id` presente.
- `active_agent_channel(ctx)` presente.

Cambios de estado en Redis:

Primer lock:

- `transfer_in_progress=True`
- `transfer_phase="requested"`
- `consultation=ConsultationData(active=True, initiator_agent_ch=A, main_bridge=bridge_id, target_agent_id, target_endpoint)`

Lock intermedio:

- `consultation.consult_bridge=consult_bridge_id`

Lock posterior al originate:

- `consultation.consult_leg_ch=consult_leg_id`

Locks distribuidos:

- No se mantiene el lock durante create bridge, move de canales ni originate.
- Hay revalidacion parcial antes del originate.

Operaciones ARI:

1. `create_bridge()` temporal.
2. MOH en main bridge.
3. `remove_channel_from_bridge(main_bridge, A)`.
4. `add_channel_to_bridge(consult_bridge, A)`.
5. `originate_channel_op(... consult_leg:true, customer_id:{call_id})` hacia B.

Canales y bridges:

- Main bridge: conserva PSTN, queda en MOH.
- Consult bridge: aloja A y luego B.

Flags afectados:

- `consultation.active=True`
- `consultation.initiator_agent_ch=A`
- `consultation.consult_bridge`
- `consultation.consult_leg_ch`

Como evita cerrar la llamada cuando cuelga el agente transferente:

- Todavia no usa `ignore_next_agent_hangup`.
- La proteccion mientras la consulta esta abierta es `transfer_in_progress=True`.

Reporte:

- No hay `log_transfer()` de inicio consultivo.

Rollback:

- Si algo falla, el `except` intenta `consult_cancel(unique_id)`.

### F.6 `consult_complete()`

Objetivo funcional:

- Completar la transferencia consultiva: B pasa al main bridge, A sale, el bridge temporal desaparece.

Precondiciones:

- `consultation.active=True`
- `consult_leg_ch` presente
- evidencia de answer:
  - `consult_leg_answered_ts`, o
  - `ARI state == Up`

Cambios de estado en Redis:

Lock de validacion:

- revalida que B sigue siendo el consult leg.
- si B sigue `Up`, marca `ignore_next_agent_hangup=True`.

Lock final:

- `finalize_current_agent_segment(ctx)` para A
- `agent_connected_channel=consult_leg_ch`
- `agent_attempt_channel=None`
- `agent_id=target_agent_id`
- `consultation=None`
- `transfer_in_progress=False`
- `is_transferred=True`
- `transfer_count += 1`
- `transfer_phase="none"`

Locks distribuidos:

- El lock no cubre `add_channel_to_bridge`, `hangup_channel(A)` ni `destroy_bridge(consult_bridge)`.

Operaciones ARI:

1. `add_channel_to_bridge(main_bridge, B)`
2. `hangup_channel(A)`
3. `DELETE /bridges/{main_bridge}/moh`
4. `destroy_bridge(consult_bridge)`

Canales y bridges:

- B pasa del consult bridge al main bridge.
- A es colgado.
- El consult bridge se destruye.

Flags afectados:

- `ignore_next_agent_hangup=True` antes de colgar A.
- `consultation` desaparece al final.

Como evita cerrar la llamada cuando cuelga el agente transferente:

- El hangup de A queda protegido por `ignore_next_agent_hangup`.
- Esa proteccion es consumida en `InboundCallHandler.on_hangup_request()` o `TransferManager.on_transfer_target_hangup()`.

Que reporta y cuando:

- Reporta una `ATTENDED OK` al final.
- `talk_time` reportado sale de `finalize_current_agent_segment()`, o sea el tramo de A.

Rollback:

- No existe rollback posterior si las operaciones ARI fallan a mitad de camino; el metodo solo retorna `False`.

### F.7 `consult_cancel()`

Objetivo funcional:

- Abortar la consulta y volver al estado A-cliente sin completar la transferencia.

Precondiciones:

- `consultation.active=True`.

Cambios de estado en Redis:

Lock inicial:

- copia `initiator_agent_ch`, `main_bridge`, `consult_leg_ch`, `consult_bridge`.

Lock final:

- `consultation=None`
- `transfer_in_progress=False`
- `transfer_phase="none"`

Operaciones ARI:

1. `add_channel_to_bridge(main_bridge, A)`
2. `hangup_channel(consult_leg_ch)`
3. detener MOH en main bridge
4. destruir consult bridge

Canales y bridges:

- A vuelve al main bridge.
- B se cuelga si existia.
- El consult bridge desaparece.

Reporte:

- No hay evento especifico de transferencia cancelada.

Rollback:

- Este metodo en si es el rollback de `consult_start()`.

### F.8 `three_way_add()`

Objetivo funcional declarado:

- Agregar un tercer agente a la llamada existente.

Precondiciones:

- `AgentStatusService` disponible.
- SIP del agente resoluble.

Operaciones ARI:

- `originate_channel_op(... three_way_leg:true, bridge_id, customer_id)`

Estado Redis:

- No actualiza contexto directamente.
- Espera que el router procese el `StasisStart` del nuevo leg.

Observacion real del codigo:

- El router actual maneja `three_way_conf_leg:true`, pero no existe una rama para `three_way_leg:true`.
- Por eso, la implementacion actual de `three_way_add()` parece incompleta o desconectada.

### F.9 `three_way_conf_add()`

Objetivo funcional:

- Agregar un tercer participante SIP externo y dejarlo registrado en `other_channels`.

Precondiciones:

- `bridge_id` existente.

Operaciones ARI:

- `originate_channel_op(... three_way_conf_leg:true, bridge_id, customer_id)`

Estado Redis:

- El cambio real ocurre en `router._handle_stasis_start()` cuando ese leg entra:
  - `add_channel_to_bridge(bridge_id, channel_id)`
  - `ctx.other_channels.append(channel_id)`

Reporte:

- No genera un reporte especifico de transferencia; es mas una extension de conferencia.

### F.10 `on_transfer_leg_start()`

Objetivo funcional:

- Consolidar la nueva pierna de una blind transfer en el bridge principal.

Precondiciones:

- Debe poder resolver `call_id` desde `customer_id`, indices de canal, `OMLUNIQUEID` o bridge.
- `transfer_in_progress=True`.
- El contexto no debe estar `call_ended=True`.

Cambios de estado en Redis:

Lock inicial:

- guarda `old_agent`
- `agent_attempt_channel=channel_id`

Lock de consolidacion tras `add_channel_to_bridge()`:

- `finalize_current_agent_segment(ctx)`
- `agent_connected_channel=channel_id`
- `agent_attempt_channel=None`
- si existe `target_agent_id`, `agent_id=target_agent_id`
- `transfer_in_progress=False`
- `transfer_count += 1`
- `is_transferred=True`
- `transfer_phase="none"`

Operaciones ARI:

1. detener MOH del bridge
2. `add_channel_to_bridge(bridge_id, channel_id)`
3. posible `hangup_channel(old_agent)` al final

Reporting:

- `OK` inmediato si el canal ya esta `Up`.
- Si no esta `Up`, deja `blind_transfer_pending_up_report=True` para que `ChannelStateChange Up` cierre el OK despues.
- Si `add_channel_to_bridge()` falla, puede reportar `FAILED`.

Actualizacion de agente:

- Si el canal ya esta `Up`, intenta `AgentStatusService.set_oncall()`.
- Si no esta `Up`, espera a `ChannelStateChange`.

Como evita cerrar la llamada por el hangup del agente anterior:

- En esta variante la defensa principal sigue siendo `transfer_in_progress` durante la ventana critica.

Rollback:

- Si no habia blind reporting activo, el `except` intenta restaurar `agent_attempt_channel=None` y `agent_id=old_agent_id`.
- Si ya habia blind reporting activo y `_report_blind_transfer_bridge_failed()` deja `transfer_in_progress=False`, ese rollback puede quedar parcial.

### F.11 `on_consult_leg_start()`

Objetivo funcional:

- Registrar el canal B consultado y sumarlo al consult bridge.

Precondiciones:

- `customer_id` en args.
- `consultation` existente.

Cambios de estado:

- `consultation.consult_leg_ch=channel_id`

Operaciones ARI:

- `add_channel_to_bridge(consult_bridge_id, channel_id)`

Reporte:

- Ninguno.

Rollback:

- Ninguno explicito dentro de este metodo.

### F.12 `on_transfer_target_hangup()`

Objetivo funcional:

- En llamadas manuales ya transferidas, si cuelga el ultimo agente actual, colgar tambien PSTN y destruir el bridge.

Precondiciones:

- Contexto manual.
- `is_transferred=True`.
- `channel_id == active_agent_channel(ctx)`.

Cambios de estado:

- Si `ignore_next_agent_hangup=True` y el canal es el iniciador consultivo, solo consume el flag y no limpia nada.

Operaciones ARI:

- `hangup_channel(pstn_channel)`
- `destroy_bridge(bridge_id)`

Ambito:

- La politica especial esta implementada solo para llamadas manuales.
- Inbound y progressive se apoyan en sus handlers de hangup regulares.

## G. Concurrencia, locks y consistencia

### G.1 Estrategia base

- Lock distribuido por `call_id` en Redis.
- No es reentrante.
- El patron correcto es:
  - leer y modificar bajo lock,
  - usar `register_unsafe()`,
  - sacar del lock las operaciones ARI lentas.

### G.2 Dos niveles de sincronizacion

Sincronizacion global por llamada:

- `CallRegistry.lock(call_id)`
- `mark_call_ended_atomic(call_id)`

Sincronizacion local del proceso:

- `_call_events_lock`
- `_dialing_lock`
- `_recordings_lock`
- locks internos de `QueueEventManager`

### G.3 Disenio consciente para carreras

Mecanismos explicitos del codigo:

- `mark_call_ended_atomic()` para que solo un thread cierre logicamente la llamada.
- `ignore_next_agent_hangup` para no matar la llamada al colgar A despues de `consult_complete`.
- `transfer_in_progress` para bloquear cleanup destructivo durante ventanas criticas.
- `call_transfer_routing_active()` para seguir tratando distinto una llamada ya transferida aunque `transfer_in_progress` ya haya bajado.
- `queue_timeout_should_suppress_cleanup()` para que un timeout tardio no destruya una llamada ya atendida.
- `CallRegistry._get_all_associated_channels()` para indexar legs auxiliares y evitar contextos invisibles.

### G.4 Estado logico versus operaciones fuera del lock

La aplicacion conscientemente acepta ventanas donde:

- Redis refleja un futuro deseado.
- ARI aun no completo la operacion fisica.

Ejemplos:

- `blind_to_endpoint()`: A ya fue colgado, pero el estado aun puede decir que sigue siendo el `agent_connected_channel`.
- `consult_start()`: A ya salio del main bridge y todavia no existe B.
- `blind_to_campaign()`: el hangup de A puede llegar antes de que la llamada quede logicamente "en cola".

## H. Riesgos, bugs potenciales e inconsistencias

### H.1 Riesgo alto: reserva de agente de 15s fija contra `ring_timeout` variable

En `DistributionService` y `VoicebotDistributionLoop` la reserva temporal usa `SET NX EX 15` sobre `acd:lock:agent:{id}`. `ring_timeout` es configurable y puede ser mayor. Si el ring sigue mas de 15 segundos y el agente sigue `READY`, otra instancia puede volver a originarlo.

Impacto:

- doble ring concurrente,
- multi-originate cross-node,
- metricas de distribucion distorsionadas.

### H.2 Riesgo alto: `blind_to_endpoint()` no compensa fallos sin leg destino

Si el agente A ya fue colgado y el originate falla de forma sincronica:

- no se restaura A,
- no se detiene MOH,
- no se inicia ninguna redistribucion alternativa,
- el cliente puede quedar solo en el bridge.

La recuperacion `recover_after_blind_transfer_leg_failed()` solo aplica cuando llego a existir una pierna y luego se destruyo.

### H.3 Riesgo alto: `consult_complete()` no actualiza explicitamente el estado ONCALL del agente B

`ChannelStateChange Up` del consult leg se consume en el bloque especial del router y retorna antes de llamar a `AgentStatusService.set_oncall()`. Luego `consult_complete()` mueve a B al main bridge pero no llama a `set_oncall()`.

Resultado posible:

- B queda atendiendo la llamada pero Redis puede seguir mostrandolo `READY`.

### H.4 Riesgo alto: `consult_complete()` no actualiza `uniqueid_agent`

Despues de completar la consultiva:

- `agent_connected_channel` pasa a B,
- `agent_id` pasa a B,
- pero `uniqueid_agent` queda apuntando a A.

Efectos potenciales:

- reportes de leg AGENT con id tecnico incorrecto,
- confusion entre "agente actual" y "agente iniciador",
- cleanup o metricas apoyadas en `uniqueid_agent` pueden usar el canal equivocado.

### H.5 Riesgo medio-alto: `three_way_add()` parece no estar cableado

`three_way_add()` origina con `three_way_leg:true`, pero `router._handle_stasis_start()` solo implementa `three_way_conf_leg:true`. No hay evidencia en el codigo de que `three_way_leg:true` sea consumido correctamente.

### H.6 Riesgo medio-alto: `blind_to_campaign()` reporta OK antes de confirmar que la nueva distribucion arranco

Si `_start_redistribution_after_blind_to_campaign()` falla:

- el metodo ya reporto `TRANSFER OK`,
- ya incremento `transfer_count`,
- ya marco `is_transferred=True`,
- luego cuelga PSTN y destruye bridge,
- pero devuelve `True`.

Eso deja una discrepancia clara entre reporter y runtime.

### H.7 Riesgo medio: `_report_blind_transfer_bridge_failed()` puede dejar campos inconsistentes

Cuando el blind reporting ya esta en modo `requested`, `_report_blind_transfer_bridge_failed()` baja `transfer_in_progress` y limpia tracking blind, pero no limpia `agent_attempt_channel`. Luego el rollback del `except` puede omitirse porque la condicion exige `transfer_in_progress=True`.

Resultado posible:

- `agent_attempt_channel` colgado pero aun indexado en Redis,
- estado logico mezclado entre agente viejo y leg fallido.

### H.8 Riesgo medio: originar fuera del lock protege la latencia, pero deja ventanas criticas reales

Esto esta explicitamente reconocido en comentarios de `transfer.py`. La mitigacion existe, pero no elimina:

- bridges temporales huerfanos,
- legs tardios de transferencia,
- hangs y cleanups cruzados si el cliente cuelga a mitad de una transferencia.

### H.9 Riesgo medio: `id_camp` y `distribution_campaign_id` son correctos conceptualmente, pero faciles de mezclar

El codigo hace el desacople de forma intencional, pero no uniforme:

- `reporter` casi siempre toma `id_camp`.
- `QueueEventManager` y `set_oncall()` pueden usar `effective_queue_campaign_id()`.
- algunos caminos siguen leyendo `ctx.id_camp` directamente.

Resultado:

- la atribucion BI puede ser correcta mientras la operacion en vivo responde a otra campania,
- pero algunos reportes operativos pueden mezclar ambos planos.

### H.10 Riesgo medio: eventos tardios pueden intentar revivir contexto

Existen varias defensas:

- `call_ended`
- validacion de PSTN en manual
- `is_channel_in_context()`

Pero siguen existiendo ventanas donde un leg tardio:

- entra a Stasis despues del cierre,
- busca contexto por indices antiguos,
- o llega antes de que `unregister()` elimine todo.

`on_transfer_leg_start()` ya contiene guardas especificas contra esto, lo que indica que el problema fue real.

### H.11 Riesgo medio: `QueueEventManager` mantiene cache propia ademas de Redis

El modulo ya implementa `_validate_state_consistency()` y `_sync_from_redis()`, lo que muestra que puede divergir del estado Redis bajo errores o carreras.

### H.12 Riesgo medio-bajo: doble cleanup por `on_transfer_target_hangup()` y handlers de hangup

En manual:

- `AcDRouter._handle_channel_hangup_request()` llama primero a `TransferManager.on_transfer_target_hangup()`,
- luego despacha a `ManualCallHandler.on_hangup_request()`.

El resultado esperable es idempotente, pero ARI puede recibir doble `hangup_channel()` o doble `destroy_bridge()`.

## I. Recomendaciones de mejora

1. Convertir la reserva de agente en un lease renovable con TTL >= `ring_timeout + margen`, o mover el agente a `RINGING` en Redis antes del originate.
2. Separar explicitamente identidad del agente actual y del agente iniciador:
   - `current_agent_channel`
   - `initiator_agent_channel`
   - `current_agent_uniqueid`
3. Hacer que `consult_complete()` actualice:
   - `uniqueid_agent`
   - estado `ONCALL` del agente B
4. Arreglar `three_way_add()` o removerlo hasta que exista una rama `three_way_leg:true` en el router.
5. En `blind_to_endpoint()`, agregar rollback fisico minimo si el originate falla sin crear leg:
   - detener MOH,
   - marcar estado de llamada para redistribucion o cierre controlado,
   - reportar compensacion.
6. En `blind_to_campaign()`, no reportar `OK` hasta que la redistribucion haya quedado efectivamente lanzada.
7. Unificar la limpieza de campos despues de fallos blind:
   - `agent_attempt_channel`
   - `agent_connected_channel`
   - `uniqueid_agent`
   - `target_agent_id`
8. Revisar todos los lugares que usan `ctx.id_camp` y decidir explicitamente si debieran usar `effective_queue_campaign_id(ctx)`.
9. Crear una maquina de estados explicita de transferencia en vez de mezclar:
   - `transfer_in_progress`
   - `transfer_phase`
   - `is_transferred`
   - `blind_transfer_report_state`
10. Agregar tests de integracion multihilo para:
   - blind fail antes de crear leg
   - consult_complete con verificacion de `ONCALL`
   - `blind_to_campaign` con redistribucion fallida
   - expiracion del lock de agente durante ring prolongado

## J. Apendice con secuencias detalladas paso a paso

### J.1 Llamada inbound normal

```text
PSTN -> StasisStart
InboundCallHandler.on_start
  -> create_bridge
  -> answer(PSTN)
  -> add PSTN to bridge
  -> start MOH
  -> create CallContext
  -> reporter.log_queue
  -> QueueEventManager.on_enter_queue
  -> DistributionService.start_distribution

DistributionService loop
  -> lee miembros de campania
  -> QueueStrategyEngine.get_candidates
  -> dial_agent_with_headers
  -> guarda agent_attempt_channel

Agente -> StasisStart(channel_type=to_agent)
InboundCallHandler.on_agent_stasis_start
  -> DistributionService.handle_agent_answer
  -> stop_distribution(cancel_timer=True)
  -> add agent to bridge
  -> agent_attempt_channel -> agent_connected_channel
  -> agent_answered_ts
  -> AgentStatusService.set_oncall
  -> stop MOH
  -> QueueEventManager.on_answered

PSTN cuelga
  -> StasisEnd PSTN
  -> on_pstn_stasis_end
  -> destroy bridge / hangup remanentes
  -> mark_call_ended_atomic
  -> finalize_current_agent_segment
  -> reporter.log_segment_end(EXIT_ANSWERED)
  -> unregister
```

### J.2 Transferencia ciega a agente

```text
A <-> Cliente en bridge principal
Comando blind_to_agent(call_id, B)
  -> resolve SIP B
  -> blind_to_endpoint
     -> lock call_id
     -> transfer_in_progress=True
     -> target_agent_id=B
     -> start MOH
     -> hangup canal A
     -> originate transfer_target:true customer_id:call_id
     -> blind_transfer_report_state=requested
     -> reporter.log_transfer(TRANSFER_REQUESTED)

B -> StasisStart transfer_target:true
TransferManager.on_transfer_leg_start
  -> encuentra contexto
  -> agent_attempt_channel=Bch
  -> stop MOH
  -> add Bch to bridge principal
  -> finalize_current_agent_segment(A)
  -> agent_connected_channel=Bch
  -> agent_id=B
  -> transfer_in_progress=False
  -> transfer_count++
  -> is_transferred=True
  -> reporter.log_transfer(OK) ahora o luego en ChannelStateChange Up
  -> hangup A si aun vive
```

### J.3 Transferencia ciega a endpoint externo

```text
A <-> Cliente
blind_to_endpoint(endpoint externo)
  -> misma mecanica que blind_to_agent
  -> target_agent_id queda None
  -> blind_transfer_numero_extra=endpoint
  -> destination_type=EXTERNAL en reporter

Leg externo entra a Stasis
on_transfer_leg_start
  -> agrega canal externo al bridge
  -> cierra segmento de A
  -> agent_connected_channel = canal externo
  -> agent_id puede quedar sin cambio si no hubo target_agent_id
  -> OK final por reporter cuando el leg queda Up
```

### J.4 Transferencia ciega a campania

```text
A <-> Cliente
blind_to_campaign(call_id, camp_dest)
  -> lock call_id
  -> transfer_in_progress=True
  -> ignore_next_agent_hangup=True
  -> start MOH
  -> hangup A
  -> re-lock
  -> finalize_current_agent_segment(A)
  -> distribution_campaign_id = camp_dest
  -> agent_id=None
  -> agent_connected_channel=None
  -> transfer_count++
  -> is_transferred=True
  -> reporter.log_transfer(OK, destination_type=CAMPAIGN)
  -> _start_redistribution_after_blind_to_campaign
     -> stop_distribution vieja
     -> QueueEventManager.on_enter_queue(camp_dest)
     -> start_distribution(camp_dest)
```

### J.5 Transferencia consultiva completa

```text
A <-> Cliente en main bridge
consult_start(call_id, B)
  -> transfer_in_progress=True
  -> consultation.active=True
  -> create consult_bridge
  -> MOH en main bridge
  -> mover A del main bridge al consult_bridge
  -> originate consult_leg hacia B

B -> StasisStart consult_leg:true
on_consult_leg_start
  -> consultation.consult_leg_ch=Bch
  -> add Bch to consult_bridge

B -> ChannelStateChange Up
router._handle_channel_state_change
  -> consultation.consult_leg_answered_ts=now
  -> transfer_phase=answered

consult_complete(call_id)
  -> valida que B este Up
  -> ignore_next_agent_hangup=True
  -> add Bch al main bridge
  -> hangup A
  -> stop MOH main
  -> destroy consult_bridge
  -> finalize_current_agent_segment(A)
  -> agent_connected_channel=Bch
  -> agent_id=B
  -> consultation=None
  -> transfer_in_progress=False
  -> transfer_count++
  -> is_transferred=True
  -> reporter.log_transfer(ATTENDED OK)
```

### J.6 Cancelacion de transferencia consultiva

```text
Consulta activa A <-> B en consult_bridge, cliente en main bridge con MOH
consult_cancel(call_id)
  -> re-add A al main bridge
  -> hangup B si existe
  -> stop MOH main
  -> destroy consult_bridge
  -> consultation=None
  -> transfer_in_progress=False
  -> transfer_phase=none
```

### J.7 Cadena de multiples `blind_to_campaign` consecutivos

Escenario realista:

- Campania original `id_camp=10`
- A atiende en campania operativa 10
- transfiere a campania 20
- B atiende en campania operativa 20
- transfiere a campania 30

Evolucion del contexto:

```text
Inicio:
  id_camp=10
  distribution_campaign_id=None
  transfer_count=0
  agent_segments=[]

Despues de A -> campania 20:
  id_camp=10
  distribution_campaign_id=20
  transfer_count=1
  agent_segments=[segmento de A]

Despues de B -> campania 30:
  id_camp=10
  distribution_campaign_id=30
  transfer_count=2
  agent_segments=[segmento de A, segmento de B]
```

Consecuencia:

- la campania de BI sigue siendo 10,
- la campania operativa viva pasa a ser 20 y luego 30,
- `call_has_prior_agent_handling()` sigue dando `True` aunque ya no haya agente conectado.

### J.8 Hangup del cliente durante una transferencia en curso

Caso mas delicado:

```text
A <-> Cliente
blind_to_endpoint en progreso
  -> transfer_in_progress=True
  -> A ya fue colgado
  -> B todavia esta ringing o entrando

Cliente cuelga
  -> ChannelDestroyed / StasisEnd PSTN
  -> handlers usan transfer_in_progress para bloquear cierres prematuros
  -> manual.py tiene una excepcion especifica para "PSTN hangup during blind transfer ringing"
  -> on_transfer_leg_start revalida call_ended y presencia de PSTN antes de consolidar
  -> si la pierna B llega tarde, debe ser ignorada
```

El codigo intenta evitar que un leg tardio "reviva" la llamada, pero acepta ventanas cortas donde el leg destino puede existir fisicamente aunque la llamada ya este cerrada.

### J.9 Hangup del agente destino despues de una blind transfer

Manual:

```text
A transfiere ciegamente a B
B queda como agente actual
B cuelga
  -> ChannelHangupRequest
  -> TransferManager.on_transfer_target_hangup
     -> detecta que call_type=manual, is_transferred=True y channel_id es el agente actual
     -> cuelga PSTN
     -> destruye bridge
  -> luego el cierre normal terminara limpiando Redis
```

Inbound/progressive:

```text
B cuelga
  -> ChannelHangupRequest
  -> handler inbound/progressive detecta leg de agente activo
  -> si no hay transfer_in_progress ni voicebot_transfer_waiting
     -> cuelga PSTN
     -> destruye bridge
```

## Conclusiones practicas

La aplicacion tiene un modelo de estado bastante rico y deliberadamente desacoplado de ARI. La mayor sofisticacion real esta en:

- deduplicacion de finales con `mark_call_ended_atomic()`,
- distincion entre `id_camp` y `distribution_campaign_id`,
- uso de `ignore_next_agent_hangup` para consultivas,
- reporting bifasico de blind transfer,
- y `agent_segments` para no perder historia tras re-encolados o transfers.

El area con mas deuda tecnica no es la distribucion normal sino la frontera entre estado logico Redis y operaciones ARI durante transferencias. Ahi hay varias decisiones correctas de diseno, pero todavia quedan huecos donde el runtime puede divergir del reporte o dejar estado parcialmente inconsistente.
