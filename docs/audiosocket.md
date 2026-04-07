# Propuesta de arquitectura para soporte de voicebots por AudioSocket en ACD/Stasis

## 1. Resumen ejecutivo

La recomendación es evolucionar la plataforma actual, centrada en `Asterisk + Stasis/ARI + ACD + Redis`, hacia un modelo de integración dual de voicebots:

- `SIP/SIP REFER` se mantiene como mecanismo soportado y backward compatible.
- `AudioSocket` se incorpora como un segundo transporte de media para bots externos.
- El control de sesión y el handoff dejan de depender exclusivamente de SIP REFER y pasan a normalizarse sobre `Redis Pub/Sub`, reutilizando el mecanismo ya existente de comandos del ACD.

La pieza nueva no debe ser un simple media loop. Debe ser un `Bot Gateway` con dos responsabilidades explícitas:

1. `media adapter` para AudioSocket.
2. `control adapter` para traducir acciones del bot a comandos internos del ACD.

Stasis/ARI debe seguir siendo el centro de orquestación porque ya concentra el ciclo de vida de la llamada, el bridge, el estado distribuido, la distribución a colas/agentes y la compatibilidad con el ACD actual. La expansión propuesta no mueve esa autoridad; agrega una capa de adaptación para nuevos transportes.

La arquitectura objetivo queda así:

```text
                   +-------------------------------+
                   |         ACD / Backoffice      |
                   | colas, agentes, campañas      |
                   +---------------+---------------+
                                   |
                                   | Redis Pub/Sub
                                   v
+----------+      ARI/Stasis    +-------------------+      AudioSocket
| Cliente  | <----------------> | Asterisk + ARI App| <----------------+
+----------+                    | session manager   |                 |
                                | bridges, state    |                 |
                                +-----+---------+---+                 |
                                      |         |                     |
                                      |         | SIP                 |
                                      |         +---------------------+----> Proveedor bot SIP
                                      |
                                      | externalMedia / chan_audiosocket
                                      v
                                +-------------------+
                                |    Bot Gateway    |
                                | AudioSocket       |
                                | control adapter   |
                                +---------+---------+
                                          |
                                          | API/WebSocket/gRPC
                                          v
                                +-------------------+
                                | Proveedor bot     |
                                | AudioSocket/AI    |
                                +-------------------+
```

## 2. Arquitectura actual

### 2.1 Estructura actual sobre Asterisk + Stasis/ARI

La base actual del componente ACD ya responde al patrón correcto para un contact center multi-tenant:

- El canal de cliente entra a `Stasis`.
- La aplicación ARI crea y administra el `bridge` principal de la conversación.
- El estado de la llamada se persiste en Redis mediante `CallRegistry`.
- La distribución a agentes humanos se resuelve en `DistributionService`.
- Las transferencias se resuelven en `TransferManager`.
- Los comandos externos se reciben por `CommandListener`, suscripto a `acd:commands:global` y `acd:commands:{NODE_ID}`.

En el árbol actual esto ya se ve reflejado en:

- `source/ari-app/sip_refer_listener.py`
- `source/ari-app/infrastructure/command_listener.py`
- `source/ari-app/services/command_dispatcher.py`
- `source/ari-app/services/distribution_service.py`
- `source/ari-app/transfer.py`
- `source/ari-app/state.py`

### 2.2 Integración actual de bots vía SIP

Hoy el bot se comporta, desde la perspectiva de Asterisk, como un endpoint SIP o un destino SIP alcanzable por trunk externo.

El flujo operativo actual es:

1. La llamada del cliente entra a Stasis.
2. La app ARI crea el `bridge`.
3. `DistributionService` puede seleccionar un agente marcado como `VOICEBOT=1`.
4. `CallActionService.dial_voicebot_with_headers()` origina una pierna SIP hacia el bot.
5. La pierna del bot entra al mismo bridge que el cliente.
6. El cliente conversa con el bot como si fuera un agente especial.

En este esquema SIP cumple dos roles:

- `media/signaling`: establecer la sesión del bot.
- `control`: permitir transferencia posterior mediante `SIP REFER`.

### 2.3 Rol de SIP REFER en el flujo actual

El listener actual `sip_refer_listener.py` ya implementa el patrón de control de handoff para voicebots SIP:

- Resuelve el `call_id` a partir del canal que origina el REFER.
- Interpreta el destino del REFER como campaña o agente.
- Si el destino es agente, dispara `blind_to_agent`.
- Si el destino es campaña, y el transferente es un voicebot, hace una liberación controlada del leg del bot:
  - saca el bot del bridge
  - cuelga su canal
  - decrementa el contador `VOICEBOT-CALLS`
  - deja al cliente en MOH
  - registra un waiter
  - espera `voicebot_transfer_proceed` por Redis o TTL
  - luego invoca `start_distribution`

Ese detalle es importante: la plataforma ya tiene un precedente concreto de handoff desacoplado del signaling SIP puro. REFER inicia la intención, pero el avance del traspaso humano ya puede coordinarse por Redis.

### 2.4 Cómo se produce hoy el handoff a cola o agente humano

Hay dos variantes:

- `bot -> agente humano`
  - el bot manda REFER a un destino que se interpreta como agente.
  - el `SipReferHandler` llama a `TransferManager.blind_to_agent()`.

- `bot -> cola/campaña`
  - el bot manda REFER a campaña.
  - la app Stasis libera el leg del bot.
  - el cliente queda en el bridge con MOH.
  - el ACD reanuda distribución humana contra la campaña objetivo.

### 2.5 Participación del ACD

El ACD participa en tres capas:

- `estado`: Redis como fuente de verdad de `CallContext`.
- `control`: `CommandListener` y `CommandDispatcher` para acciones sobre llamadas activas.
- `negocio`: colas, agentes, timers, reportes, límites y estrategia de distribución.

Conclusión sobre el estado actual: la arquitectura ya está correctamente centrada en Stasis/ARI y Redis. La extensión a AudioSocket debe apoyarse ahí y no crear un plano de control paralelo.

## 3. Arquitectura objetivo

### 3.1 Principio rector

La plataforma debe soportar dos modos de integración de voicebots sin cambiar el core del ACD:

- `modo SIP`: bot conectado por SIP, con opción de control por SIP REFER y/o Redis.
- `modo AudioSocket`: bot conectado por streaming PCM bidireccional, con control por Redis.

La decisión de transporte debe ser una propiedad de configuración del bot o de la campaña, no del flujo core de atención.

### 3.2 Modelo propuesto

La propuesta es introducir un `Bot Session Manager` en la app ARI y un `Bot Gateway` externo para AudioSocket.

```text
                   +------------------------------------+
                   |          ARI / Stasis App          |
                   |------------------------------------|
                   | Bot Session Manager                |
                   | TransferManager                    |
                   | DistributionService                |
                   | CommandDispatcher                  |
                   | CallRegistry                       |
                   +-----------------+------------------+
                                     |
                         selecciona adapter por transporte
                                     |
                +--------------------+--------------------+
                |                                         |
                v                                         v
      +------------------------+              +------------------------+
      | SipBotAdapter          |              | AudioSocketBotAdapter  |
      | usa originate SIP      |              | usa externalMedia      |
      | REFER opcional         |              | contra Bot Gateway     |
      +-----------+------------+              +-----------+------------+
                  |                                           |
                  v                                           v
      +------------------------+              +------------------------+
      | Proveedor bot SIP      |              | Bot Gateway            |
      | B2BUA / SBC / UA       |              | sesión + media + ctrl  |
      +------------------------+              +-----------+------------+
                                                          |
                                                          v
                                              +------------------------+
                                              | Proveedor bot IA       |
                                              +------------------------+
```

### 3.3 Cómo incorporar AudioSocket

Para AudioSocket no propongo sacar la llamada de Stasis ni resolver el bot por dialplan puro. Propongo usar `ARI externalMedia` con `transport=tcp` y `encapsulation=audiosocket`, de forma que el canal externo siga estando bajo control de la misma app Stasis.

Eso permite:

- mantener un único orquestador de llamada
- seguir usando el bridge actual
- conservar trazabilidad completa en ARI
- evitar un bypass del estado del ACD

El flujo recomendado es:

1. El cliente entra a Stasis.
2. `Bot Session Manager` resuelve que el bot usa `transport=audiosocket`.
3. La app crea un `externalMedia channel` contra el `Bot Gateway`.
4. Ese canal entra a la misma app Stasis.
5. El canal AudioSocket se agrega al `bridge` de la conversación.
6. El `Bot Gateway` traduce el stream AudioSocket al protocolo requerido por el proveedor bot.
7. El `Bot Gateway` publica acciones de control en Redis.
8. La app Stasis ejecuta handoff, cierre o fallback sobre la llamada real.

### 3.4 ¿Media loop, media proxy, media adapter o bot gateway?

La decisión correcta es `Bot Gateway`.

No alcanza con modelarlo como `media loop`:

- un media loop solo reenvía audio
- no modela identidad de sesión
- no resuelve control, handoff, tenant, timeouts ni trazabilidad

Tampoco conviene pensarlo solo como `media proxy`:

- el problema no es solo pasar RTP/PCM
- hay que transformar acciones de negocio del bot a comandos del ACD

El `AudioSocketBotAdapter` sí es un `media adapter`, pero encapsulado dentro de un `Bot Gateway`.

La definición recomendada es:

- `Bot Gateway`: componente de borde para sesiones bot, consciente de tenant, bot_session_id, control y observabilidad.
- `AudioSocket adapter`: submódulo del gateway que termina el protocolo AudioSocket.

Justificación:

- desacopla transporte de la lógica de handoff
- evita meter lógica específica de proveedores IA dentro del ARI app
- permite soportar varios vendors sin contaminar `TransferManager` ni `DistributionService`
- habilita estrategias mixtas: SIP directo, AudioSocket, e incluso futuros transports

### 3.5 Componentes nuevos

Los componentes nuevos mínimos son:

1. `Bot Session Manager` en la app ARI.
2. `Bot Adapter` como contrato interno.
3. `AudioSocketBotAdapter` en la app ARI.
4. `Bot Gateway` externo.
5. `Bot Command Publisher` para Redis Pub/Sub.
6. `Bot Provider Registry` o configuración por tenant/campaña/bot.

## 4. Componentes

### 4.1 Vista de componentes

| Componente | Responsabilidad principal | Estado |
| --- | --- | --- |
| Asterisk | Media anchor, bridges, canales, signaling SIP, external media | Existente |
| App ARI/Stasis | Orquestación central de llamada y estado | Existente |
| ACD | Lógica de negocio de campañas, colas y agentes | Existente |
| Redis Pub/Sub | Canal de comandos y handoff | Existente |
| CallRegistry | Estado distribuido por `call_id` | Existente |
| CommandListener/Dispatcher | Ejecución de comandos externos | Existente |
| SipReferListener | Compatibilidad con bots SIP actuales | Existente |
| Bot Session Manager | Abstracción de sesión bot desacoplada del transporte | Nuevo |
| Bot Adapter | Contrato interno común | Nuevo |
| AudioSocketBotAdapter | Crear y administrar sesiones bot por externalMedia AudioSocket | Nuevo |
| Bot Gateway | Terminar AudioSocket, adaptar media/control a proveedor | Nuevo |
| Proveedor bot SIP | Bot externo integrado por SIP | Existente |
| Proveedor bot AudioSocket | Bot externo integrado por streaming | Nuevo |
| Colas / agentes humanos | Destino de handoff y fallback | Existente |

### 4.2 Responsabilidades por componente

#### Asterisk

- Terminar llamada del cliente.
- Crear bridge principal.
- Crear canal SIP del bot cuando aplique.
- Crear external media channel AudioSocket cuando aplique.
- Mantener el audio anclado en la plataforma.

#### App ARI/Stasis

- Mantener autoridad del estado de llamada.
- Crear `BotSession`.
- Asociar `bot_session_id`, `call_id`, `bridge_id`, `channel_id`.
- Decidir handoff, fallback, cierre y limpieza.
- Ejecutar MOH, distribución y transferencias.

#### Bot Session Manager

- Seleccionar adapter por `transport`.
- Crear/destruir sesiones bot.
- Normalizar eventos entre SIP y AudioSocket.
- Publicar/consumir metadatos comunes.

#### Bot Gateway

- Aceptar conexión AudioSocket desde Asterisk.
- Mapear UUID AudioSocket a `bot_session_id`.
- Convertir PCM/DTMF al protocolo del proveedor IA.
- Recibir acciones del bot y traducirlas al contrato interno.
- Publicar comandos de handoff por Redis.

#### Redis Pub/Sub

- Transportar comandos de control de bot hacia el ACD.
- Permitir handoff desacoplado del transporte de media.
- Mantener el mecanismo único de activación sobre llamadas activas.

## 5. Flujos

### 5.1 Llamada atendida por bot vía SIP

```text
Cliente -> Asterisk: llamada inbound/outbound conectada
Asterisk -> Stasis App: StasisStart canal cliente
Stasis App -> CallRegistry: crea CallContext
Stasis App -> Asterisk: crea bridge principal
Stasis App -> DistributionService: resuelve bot SIP para campaña
DistributionService -> Asterisk: originate SIP leg a voicebot
Asterisk -> Proveedor bot SIP: INVITE con headers de negocio
Proveedor bot SIP -> Asterisk: 200 OK
Asterisk -> Stasis App: StasisStart canal bot
Stasis App -> Asterisk: agrega bot al bridge
Cliente <-> Bot SIP: conversación
```

### 5.2 Handoff vía SIP REFER

```text
Bot SIP -> Asterisk: REFER destino campaña/agente
Asterisk -> Stasis App: ChannelTransfer / evento REFER
Stasis App -> SipReferListener: parsea destino

Si destino = agente:
  SipReferListener -> TransferManager: blind_to_agent(call_id, agent_id)
  TransferManager -> Asterisk: origina pierna agente
  Asterisk -> Stasis App: agente entra a bridge

Si destino = campaña:
  SipReferListener -> Asterisk: remueve y cuelga leg bot
  SipReferListener -> Asterisk: activa MOH en bridge cliente
  SipReferListener -> DistributionService: registra waiter
  Bot/Backend -> Redis Pub/Sub: voicebot_transfer_proceed
  CommandListener -> CommandDispatcher: despierta waiter
  DistributionService -> ACD: inicia distribución humana
```

### 5.3 Llamada atendida por bot vía AudioSocket

```text
Cliente -> Asterisk: llamada conectada
Asterisk -> Stasis App: StasisStart canal cliente
Stasis App -> CallRegistry: crea CallContext
Stasis App -> Asterisk: crea bridge principal
Stasis App -> Bot Session Manager: start_bot_session(transport=audiosocket)
Bot Session Manager -> AudioSocketBotAdapter: create session
AudioSocketBotAdapter -> Asterisk ARI: POST /channels/externalMedia
ARI externalMedia -> Bot Gateway: abre TCP AudioSocket
Bot Gateway -> Proveedor bot IA: abre stream de media/control
Asterisk -> Stasis App: StasisStart canal external media
Stasis App -> Asterisk: agrega canal AudioSocket al bridge
Cliente <-> Asterisk <-> AudioSocket <-> Bot Gateway <-> Bot IA: conversación
```

### 5.4 Handoff vía Redis Pub/Sub hacia cola humana

```text
Bot IA -> Bot Gateway: intent=handoff_to_queue
Bot Gateway -> Redis Pub/Sub: publish action=bot_handoff_queue
CommandListener -> CommandDispatcher: consume comando
CommandDispatcher -> Bot Session Manager: close bot leg
Bot Session Manager -> Asterisk: remueve externalMedia del bridge
CommandDispatcher -> Asterisk: MOH en bridge cliente
CommandDispatcher -> DistributionService: start_distribution(campaign_id)
DistributionService -> ACD: busca agentes humanos
Agente -> Asterisk/Stasis: responde
Stasis App -> Asterisk: agrega agente humano al bridge
Cliente <-> Agente: atención humana
```

### 5.5 Fallback si el bot no responde

```text
Stasis App -> AudioSocketBotAdapter: inicia sesión bot
AudioSocketBotAdapter -> Bot Gateway: connect

Si connect timeout o bot health fail:
  AudioSocketBotAdapter -> Bot Session Manager: session_failed(reason)
  Bot Session Manager -> CommandDispatcher: fallback_to_human
  CommandDispatcher -> Asterisk: MOH si aplica
  CommandDispatcher -> DistributionService: start_distribution(default_campaign)

Si bot se cae durante conversación:
  Bot Gateway -> Redis Pub/Sub: bot_fallback_human
  CommandDispatcher -> Asterisk: limpia leg bot
  CommandDispatcher -> DistributionService: distribución humana
```

### 5.6 Finalización normal de la conversación

```text
Bot IA -> Bot Gateway: action=end_session
Bot Gateway -> Redis Pub/Sub: bot_session_end
CommandDispatcher -> Bot Session Manager: close session
Bot Session Manager -> Asterisk: remueve/cuelga canal bot

Si política = hangup total:
  Stasis App -> Asterisk: cuelga canal cliente

Si política = postbot workflow:
  Stasis App -> siguiente estado:
    - encuesta
    - IVR final
    - cierre administrativo
```

### 5.7 Flujo general recomendado

```text
                      +----------------------+
                      |   Inicio de llamada  |
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      | Stasis crea contexto |
                      +----------+-----------+
                                 |
                                 v
                 +---------------+---------------+
                 | Resolver bot transport/profile|
                 +---------------+---------------+
                                 |
                 +---------------+---------------+
                 |                               |
                 v                               v
        +------------------+            +----------------------+
        | SIP Bot Adapter  |            | AudioSocket Adapter  |
        +--------+---------+            +----------+-----------+
                 |                                 |
                 v                                 v
        +------------------+            +----------------------+
        | Conversación bot |            | Conversación bot     |
        +--------+---------+            +----------+-----------+
                 |                                 |
                 +---------------+-----------------+
                                 |
                                 v
                     +---------------------------+
                     | Acción bot: handoff/end   |
                     | Control normalizado Redis |
                     +-------------+-------------+
                                   |
                                   v
                     +---------------------------+
                     | ACD ejecuta transferencia |
                     | o cierre final            |
                     +---------------------------+
```

## 6. Contratos internos

### 6.1 Modelo conceptual de BotSession

```ts
type BotTransport = "sip" | "audiosocket";

type BotSessionState =
  | "CREATED"
  | "CONNECTING"
  | "ACTIVE"
  | "HANDOFF_REQUESTED"
  | "HANDOFF_IN_PROGRESS"
  | "TERMINATING"
  | "ENDED"
  | "FAILED";

interface BotSession {
  bot_session_id: string;
  tenant_id: string;
  call_id: string;
  bridge_id: string;
  customer_channel_id: string;
  bot_channel_id?: string;
  transport: BotTransport;
  provider_id: string;
  campaign_id: string;
  agent_id?: string;
  state: BotSessionState;
  metadata: Record<string, string>;
  created_at: string;
}
```

### 6.2 Contrato `BotAdapter`

```ts
interface BotAdapter {
  startSession(req: StartBotSessionRequest): Promise<StartBotSessionResult>;
  sendMediaEvent(evt: MediaEvent): Promise<void>;
  receiveBotAction(action: BotAction): Promise<void>;
  requestHandoff(req: HandoffRequest): Promise<void>;
  closeSession(req: CloseBotSessionRequest): Promise<void>;
}
```

### 6.3 Iniciar sesión bot

```json
{
  "operation": "start_bot_session",
  "tenant_id": "tenant-a",
  "call_id": "1767206550.4",
  "bot_session_id": "bs_01J...",
  "campaign_id": "11",
  "transport": "audiosocket",
  "provider_id": "bot-verloop-audiosocket",
  "customer_channel_id": "1739387599.22",
  "bridge_id": "bridge-4f1a",
  "media": {
    "codec": "slin",
    "sample_rate_hz": 8000,
    "dtmf": true
  },
  "handoff_policy": {
    "default_campaign_id": "11",
    "allow_agent_transfer": true,
    "allow_queue_transfer": true
  },
  "metadata": {
    "id_customer": "26",
    "phone_number": "54911...",
    "call_type": "3"
  }
}
```

### 6.4 Enviar eventos de media/speech

No conviene exponer al core del ACD frames crudos. El core debe recibir eventos lógicos.

```json
{
  "operation": "bot_media_event",
  "bot_session_id": "bs_01J...",
  "call_id": "1767206550.4",
  "event_type": "speech.partial",
  "timestamp": "2026-03-10T18:04:11.221Z",
  "payload": {
    "text": "quiero hablar con una persona",
    "confidence": 0.94,
    "vendor_trace_id": "vx-8891"
  }
}
```

### 6.5 Recibir acciones del bot

```json
{
  "operation": "bot_action",
  "bot_session_id": "bs_01J...",
  "call_id": "1767206550.4",
  "action": "handoff_to_queue",
  "reason": "user_requested_human",
  "payload": {
    "target_campaign_id": "11",
    "queue_priority": "normal",
    "context": {
      "intent": "speak_to_agent",
      "summary": "cliente pide operador"
    }
  }
}
```

### 6.6 Solicitar handoff

```ts
interface HandoffRequest {
  bot_session_id: string;
  call_id: string;
  tenant_id: string;
  target_type: "queue" | "agent" | "human_fallback";
  target_campaign_id?: string;
  target_agent_id?: string;
  reason: string;
  context?: Record<string, string>;
}
```

### 6.7 Cerrar sesión

```json
{
  "operation": "close_bot_session",
  "bot_session_id": "bs_01J...",
  "call_id": "1767206550.4",
  "reason": "normal_end",
  "close_customer_call": false
}
```

### 6.8 Publicar comando de transferencia en Redis

```ts
interface TransferCommand {
  command_id: string;
  timestamp: string;
  source: "bot-gateway" | "sip-refer-listener" | "ari-app";
  tenant_id: string;
  node_id?: string;
  call_id: string;
  bot_session_id?: string;
  action:
    | "bot_handoff_queue"
    | "bot_handoff_agent"
    | "bot_session_end"
    | "bot_fallback_human"
    | "voicebot_transfer_proceed";
  payload: Record<string, unknown>;
}
```

## 7. Redis Pub/Sub para handoff

### 7.1 Decisión

Para el escenario nuevo, `Redis Pub/Sub` debe ser el mecanismo principal de handoff. SIP REFER queda soportado, pero no es el único ni el preferido para integraciones AudioSocket.

### 7.2 Motivación

Redis ya existe en el ACD como:

- bus de comandos
- fuente de estado distribuido
- mecanismo de coordinación entre nodos

Eso reduce impacto de implementación y evita crear otro canal de control.

### 7.3 Formato lógico de mensajes

Propongo publicar sobre los canales ya soportados:

- `acd:commands:global`
- `acd:commands:{NODE_ID}` cuando la llamada ya esté afinada a un nodo

Payload canónico:

```json
{
  "command_id": "cmd_01J8XYZ",
  "timestamp": "2026-03-10T18:14:00.000Z",
  "source": "bot-gateway",
  "tenant_id": "tenant-a",
  "call_id": "1767206550.4",
  "bot_session_id": "bs_01J...",
  "action": "bot_handoff_queue",
  "payload": {
    "target_campaign_id": "11",
    "reason": "user_requested_human",
    "transport": "audiosocket",
    "provider_id": "verloop-audiosocket",
    "call_summary": "cliente pidió operador",
    "intent": "speak_to_agent"
  }
}
```

Mensajes concretos por acción:

#### Transferencia a cola

```json
{
  "command_id": "cmd_queue_001",
  "timestamp": "2026-03-10T18:14:00.000Z",
  "source": "bot-gateway",
  "tenant_id": "tenant-a",
  "call_id": "1767206550.4",
  "bot_session_id": "bs_01J...",
  "action": "bot_handoff_queue",
  "payload": {
    "target_campaign_id": "11",
    "reason": "handoff_to_queue"
  }
}
```

#### Transferencia a agente

```json
{
  "command_id": "cmd_agent_001",
  "timestamp": "2026-03-10T18:14:03.000Z",
  "source": "bot-gateway",
  "tenant_id": "tenant-a",
  "call_id": "1767206550.4",
  "bot_session_id": "bs_01J...",
  "action": "bot_handoff_agent",
  "payload": {
    "target_agent_id": "1001",
    "reason": "premium_customer"
  }
}
```

#### Finalización de sesión bot

```json
{
  "command_id": "cmd_end_001",
  "timestamp": "2026-03-10T18:14:10.000Z",
  "source": "bot-gateway",
  "tenant_id": "tenant-a",
  "call_id": "1767206550.4",
  "bot_session_id": "bs_01J...",
  "action": "bot_session_end",
  "payload": {
    "reason": "conversation_completed",
    "close_customer_call": true
  }
}
```

#### Fallback a operador humano

```json
{
  "command_id": "cmd_fallback_001",
  "timestamp": "2026-03-10T18:14:15.000Z",
  "source": "bot-gateway",
  "tenant_id": "tenant-a",
  "call_id": "1767206550.4",
  "bot_session_id": "bs_01J...",
  "action": "bot_fallback_human",
  "payload": {
    "target_campaign_id": "11",
    "reason": "bot_timeout"
  }
}
```

### 7.4 Coordinación entre bot, Stasis y ACD

El modelo recomendado es:

1. `Bot Gateway` recibe una acción del proveedor bot.
2. `Bot Gateway` publica comando canónico en Redis.
3. `CommandListener` lo consume en la app ARI.
4. `CommandDispatcher` lo traduce a operación interna:
   - `bot_handoff_queue` -> liberar bot + MOH + `start_distribution`
   - `bot_handoff_agent` -> liberar bot + `blind_to_agent`
   - `bot_session_end` -> cerrar leg bot y finalizar o continuar flujo
   - `bot_fallback_human` -> liberar bot + distribución humana por defecto
5. `CallRegistry` actualiza `BotSession` y `CallContext`.
6. `TransferManager` y `DistributionService` ejecutan la acción real.

### 7.5 Convivencia entre SIP REFER y Redis

Modelo híbrido recomendado:

- `SIP bot legado`
  - media por SIP
  - control por SIP REFER

- `SIP bot evolucionado`
  - media por SIP
  - control por Redis

- `AudioSocket bot`
  - media por AudioSocket
  - control por Redis

La regla del core del ACD debe ser:

- el `transporte de media` no define el `mecanismo de handoff`
- el `mecanismo de handoff` preferido es Redis
- `SIP REFER` se mantiene por compatibilidad

## 8. Compatibilidad SIP + AudioSocket

### 8.1 Abstracción común

El core del ACD no debe depender de si el bot fue alcanzado por SIP o por AudioSocket. Debe depender solo de una abstracción `BotSession`.

Campos mínimos comunes:

- `bot_session_id`
- `call_id`
- `bridge_id`
- `tenant_id`
- `transport`
- `provider_id`
- `bot_channel_id`
- `state`
- `handoff_capabilities`

### 8.2 Contrato común de ejecución

El core solo necesita cinco operaciones:

1. `start_bot_session`
2. `notify_bot_event`
3. `request_handoff`
4. `close_bot_session`
5. `handle_bot_failure`

Todo lo demás es trabajo del adapter.

### 8.3 Backward compatibility

La compatibilidad se preserva así:

- `sip_refer_listener.py` no se elimina.
- El flujo actual de `dial_voicebot_with_headers()` se mantiene.
- Los payloads actuales de `CommandDispatcher` siguen válidos.
- Los nuevos mensajes Redis se agregan como acciones nuevas, no reemplazan las existentes.
- Las campañas existentes pueden seguir configuradas como `bot_transport=sip`.

### 8.4 Estrategia de configuración por tenant

Cada tenant o campaña debería poder definir:

- `bot_enabled`
- `bot_transport = sip | audiosocket`
- `bot_provider_id`
- `bot_endpoint`
- `handoff_default_campaign_id`
- `fallback_policy`
- `sample_rate_hz`
- `connect_timeout_ms`
- `bot_action_timeout_ms`

## 9. Riesgos y tradeoffs

### 9.1 Por qué Stasis/ARI debe seguir siendo el centro

Porque ya es el punto donde viven:

- ciclo de vida de la llamada
- bridges
- transferencias
- distribución
- estado
- reporting

Mover la orquestación fuera de Stasis duplicaría estado y abriría carreras entre Asterisk y el bot runtime.

### 9.2 Por qué desacoplar transporte de media de lógica de negocio

Porque SIP y AudioSocket resuelven cosas distintas:

- SIP resuelve signaling y establecimiento de sesiones telephony-grade.
- AudioSocket resuelve streaming PCM simple hacia motores IA.

La lógica de negocio del ACD no debe saber si el bot usa INVITE, REFER o frames PCM. Debe operar con eventos de alto nivel:

- bot conectado
- bot falló
- bot pide handoff
- bot finalizó

### 9.3 Por qué Redis Pub/Sub es adecuado

Porque:

- ya está en la plataforma
- ya existe listener y dispatcher
- es rápido para control intra-plataforma
- encaja con una arquitectura multi-nodo
- elimina dependencia del signaling SIP para handoff

Tradeoff:

- Pub/Sub puro no garantiza replay. Para comandos críticos conviene agregar `command_id`, idempotencia y opcionalmente persistencia complementaria en Redis key/stream si más adelante se requiere auditoría estricta.

### 9.4 Cuándo SIP seguirá siendo útil

- bots que ya exponen UA SIP o SBC
- integraciones donde el proveedor ya soporta REFER/transfer
- escenarios con carrier/SBC, NAT, topologías de telefonía clásicas
- migraciones de bajo impacto donde no conviene tocar media plane

### 9.5 Cuándo AudioSocket aporta más flexibilidad

- bots basados en STT/TTS/LLM que no quieren implementar SIP
- proveedores IA que operan sobre PCM o WebSocket/gRPC
- integraciones donde se quiera controlar speech, barge-in y vendor switching en el gateway
- casos donde se necesite desacoplar completamente media del control de negocio

### 9.6 Riesgos técnicos principales

- `latencia`: transcoding y salto extra Asterisk -> Gateway -> Bot.
- `resiliencia`: caída del Bot Gateway no debe colgar la llamada sin fallback.
- `capacidad`: AudioSocket a gran escala implica CPU de codec, sockets y backpressure.
- `observabilidad`: sin correlación fuerte, debugging de handoff será muy costoso.
- `aislamiento multi-tenant`: no mezclar configuración, credenciales ni colas entre tenants.

## 10. Recomendación final

### 10.1 Requisitos no funcionales

#### Latencia

- objetivo de media agregado por plataforma: `< 80 ms` unidireccional
- objetivo bot action -> comando Redis -> ejecución ACD: `< 300 ms`
- baseline sugerido: `slin/8kHz` para minimizar transcoding en PSTN
- habilitar `16kHz` solo por perfil de bot y con sizing explícito

#### Resiliencia

- `connect_timeout_ms` corto para sesión bot
- `bot_action_timeout_ms` por intent/handoff
- circuit breaker por proveedor bot
- fallback automático a humano si el bot no conecta o no responde
- Bot Gateway stateless y escalable horizontalmente

#### Reintentos

- no hacer retries infinitos sobre una llamada viva
- máximo 1 retry rápido para conexión inicial del bot
- cualquier falla posterior debe derivar a fallback o cierre ordenado

#### Observabilidad

Trazas y métricas obligatorias con:

- `tenant_id`
- `call_id`
- `bot_session_id`
- `bridge_id`
- `customer_channel_id`
- `bot_channel_id`
- `provider_id`
- `transport`
- `command_id`

Métricas mínimas:

- sesiones bot iniciadas/fallidas
- tiempo de conexión bot
- tiempo de handoff
- fallbacks automáticos
- latencia media bot/gateway
- errores por proveedor/tenant

#### Escalabilidad multi-tenant

- configuración por tenant/campaña
- pools y límites por tenant
- rate limiting para evitar que un tenant monopolice el gateway
- aislamiento de `bot_session_id` y credenciales por espacio lógico

#### Seguridad

- allowlist o mTLS entre Asterisk y Bot Gateway
- autenticación entre Bot Gateway y proveedor bot
- no exponer Redis directamente a proveedores externos
- sanitizar metadata y headers
- minimizar PII en Pub/Sub

#### Manejo de errores

- todo comando de bot debe ser idempotente por `command_id`
- toda limpieza de sesión debe tolerar doble ejecución
- si el bot leg cae, la llamada del cliente debe permanecer viva mientras exista política de fallback

### 10.2 Arquitectura recomendada para llevar a ingeniería

La arquitectura concreta recomendada es:

1. Mantener `Stasis/ARI` como plano de orquestación único.
2. Mantener `SIP` como integración soportada sin cambios disruptivos.
3. Incorporar `Bot Session Manager` y contrato `BotAdapter`.
4. Implementar `AudioSocketBotAdapter` usando `ARI externalMedia` con `chan_audiosocket`.
5. Introducir un `Bot Gateway` externo, stateless, multi-tenant y observable.
6. Normalizar el handoff a `Redis Pub/Sub`.
7. Extender `CommandDispatcher` con acciones de bot:
   - `bot_handoff_queue`
   - `bot_handoff_agent`
   - `bot_session_end`
   - `bot_fallback_human`
8. Mantener `SIP REFER` como compatibilidad y opción híbrida.

### 10.3 Tradeoff final

Se agrega un componente nuevo, `Bot Gateway`, y con eso aumenta la complejidad operacional. Ese costo es correcto porque compra cuatro beneficios estructurales:

- desacople real entre transporte y negocio
- soporte simultáneo SIP + AudioSocket
- handoff consistente independientemente del proveedor bot
- base limpia para escalar voicebots multi-tenant en producción

### 10.4 Recomendación ejecutiva

La plataforma no debe elegir entre SIP y AudioSocket. Debe soportar ambos bajo un mismo modelo de `BotSession`, con `Stasis/ARI` como autoridad, `Redis Pub/Sub` como canal de handoff y `Bot Gateway` como punto de adaptación para AudioSocket.

Ese es el diseño más evolutivo, compatible con producción y con menor riesgo de romper el ACD actual.

## Referencias técnicas

- Asterisk ARI `externalMedia`: [Channels REST API](https://docs.asterisk.org/Latest_API/API_Documentation/Asterisk_REST_Interface/Channels_REST_API/)
- Asterisk `AudioSocket` protocol: [AudioSocket](https://docs.asterisk.org/Configuration/Channel-Drivers/AudioSocket/)
