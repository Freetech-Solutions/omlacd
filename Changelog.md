# Changelog y Documentación Técnica: oml-773-dev-oml-3

## Resumen Ejecutivo
La rama `oml-773-dev-oml-3` introduce una evolución arquitectónica fundamental para el componente ACD de OMniLeads, marcando la transición hacia la versión 3.0 de la plataforma. Este cambio reemplaza la dependencia de configuraciones estáticas de dialplan y scripts AGI por una arquitectura asíncrona controlada por eventos utilizando Asterisk REST Interface (ARI) en Python, mejorando drásticamente el rendimiento, la escalabilidad y la gestión del estado en tiempo real.

## Nuevas Funcionalidades (Features)
* **Gestión de Llamadas Basada en ARI:** Toda la lógica de enrutamiento, distribución, transferencias e interacciones complejas de llamadas es manejada ahora por un motor moderno en Python (ARI), permitiendo flujos más flexibles.
* **Integración de VoiceBots:** Se incorpora la capacidad nativa de interactuar con VoiceBots. Éstos pueden ser asignados a campañas y sus métricas de interacción se consolidan de manera unificada en los reportes y tableros de supervisión junto a los agentes humanos.
* **Transferencias y Supervisión Avanzadas:** Capacidades de transferencia directa/consultiva entre campañas y agentes, y facilidades de "channel spy" completamente reescritas para funcionar mediante comandos asíncronos en la nueva arquitectura.

## Cambios Arquitectónicos / Técnicos

### Cambio de Paradigma: De Dialplan (extensions.conf) + FastAGI hacia Python ARI
Históricamente, la lógica de llamadas de OMniLeads residía en extensos y complejos archivos de dialplan (`extensions.conf`), donde cada salto de contexto invocaba scripts externos a través de **FastAGI** para resolver decisiones de negocio (autenticación de agente, lookup de campaña, registro de eventos, etc.). Este modelo combinaba dos limitaciones críticas:
* **El dialplan como lenguaje de orquestación:** la lógica vivía repartida entre cientos de extensiones y prioridades en archivos `.conf`, dificultando la lectura, el versionado y el debugging.
* **FastAGI sincrónico y bloqueante:** cada llamada abría una conexión TCP hacia el proceso AGI, que respondía instrucción por instrucción de manera serializada. Bajo concurrencia elevada, el tiempo de ida y vuelta entre Asterisk y el AGI se convertía en cuello de botella, y un AGI lento podía bloquear el canal completo.

En la nueva versión, el dialplan se ha reducido al mínimo necesario (principalmente para enrutar llamadas hacia la aplicación `Stasis` de Asterisk) y FastAGI ha sido reducido a una minima porción del flujo inbound del ACD.

A partir de la entrada a Stasis, el ciclo de vida completo de la llamada —recepción, encolamiento, reproducción de audios y finalización— es controlado de forma asíncrona mediante un servicio de Python que consume la API de ARI. Esto permite manejar la concurrencia nativamente (mediante `asyncio`), separar la lógica de negocio del PBX, y responder a eventos en tiempo real con mayor granularidad.

### Optimizaciones Generales y Naturaleza "Stateless"
El ACD ha sido refactorizado para ser inherentemente *stateless* (sin estado). Anteriormente, el estado de las llamadas y la disponibilidad de los agentes dependía fuertemente de la memoria interna de Asterisk y la propagación de eventos AMI. Esta dependencia creaba cuellos de botella y "race conditions" bajo alta carga.
Ahora, Asterisk actúa únicamente como un "motor de medios mudo", delegando todo el estado de negocio a **Redis**. Redis se consolida como la única fuente de la verdad para el estado de las colas, agentes logueados y distribución en tiempo real, lo que optimiza los recursos de Asterisk y asegura alta disponibilidad y escalabilidad horizontal.

### VoiceBots en Modelos de Agente, Reportes y Campañas
La estructura de datos ha sido modificada para que los VoiceBots funcionen como ciudadanos de primera clase dentro del sistema ("first-class citizens"). Los VoiceBots ahora pueden:
* Ser asignados a campañas específicas (tanto entrantes como salientes).
* Interactuar con el ACD simulando las capacidades de un agente humano.
* Sus eventos de inicio/fin de interacción y tipificaciones son procesados e inyectados en la reportería general, ofreciendo una métrica unificada para medir la productividad del centro de contactos, independientemente de si la atención es humana o automatizada.

### Cómo funciona el ACD (Discar, Transferir, Channel Spy)
El backend (Django) abandona la emisión de acciones vía AMI. En su lugar, interactúa con **colas Gearman** o endpoints del motor ARI.
* **Para Discar (Dial):** El backend encola la orden de discado en una **cola Gearman**, depositando allí la metadata completa de la llamada (campaña, contacto, agente destino, parámetros de origen, etc.). El servicio ARI (`Dialing Service`) actúa como *worker* Gearman: consume la orden de la cola, instruye a Asterisk a originar un canal hacia el destino e intercepta el evento de respuesta para unirlo dinámicamente al *bridge* con el agente. El uso de Gearman como bus de trabajo permite desacoplar el ritmo de producción del backend del ritmo de consumo del motor de discado, distribuir naturalmente la carga entre múltiples workers ARI y obtener *back-pressure* automático ante picos de demanda.
* **Para Transferir:** Se manipulan directamente los "bridges" mediante la API REST de ARI. Si un agente requiere transferir, la UI notifica al backend, el cual ordena al servicio ARI crear el canal de destino. Luego, el motor saca al agente del *bridge* actual y mueve a la persona que llama al nuevo flujo, controlando los estados sin requerir saltos de contexto (`GOTO`) en el dialplan.
* **Channel Spy (Supervisión):** Se implementa usando la capacidad nativa de canales *snoop* de ARI. Se crea un canal espejo ("Snoop channel") sobre la llamada del agente, y se instruye al PBX para dirigir el flujo de medios hacia el dispositivo del supervisor de manera invisible para el cliente.

### Simplificación del Entrypoint y NAT solucionado por Kamailio RTPengine
El script de arranque del contenedor Asterisk (`entrypoint.sh`) ha sido drásticamente simplificado. Previamente, contenía rutinas complejas para la detección y configuración de redes e IPs públicas para sortear problemas de NAT. 
Toda esa complejidad ha sido eliminada delegando el manejo estricto del NAT (Network Address Translation) y el ruteo de medios a **Kamailio + RTPengine** operando en el perímetro (SBC). Asterisk ahora recibe el tráfico SIP limpio, lo que reduce las directivas en `pjsip.conf` y evita la configuración engorrosa de ICE/STUN en el PBX.

### Endpoints de Agente vía Kamailio (Outbound Proxy) — Eliminación de `oml_pjsip_agents.conf`
En la arquitectura previa, cada agente requería un *endpoint* PJSIP declarado estáticamente dentro del archivo `oml_pjsip_agents.conf` (generado y mantenido por el backend para cada usuario del sistema). Esto significaba que Asterisk era simultáneamente el **registrar SIP** de los softphones y el motor de medios, acoplando ambos roles en un mismo proceso. Las consecuencias eran:
* El archivo crecía linealmente con la cantidad de agentes, llegando a tamaños inmanejables en instalaciones grandes.
* Cada alta/baja/modificación de usuario disparaba la regeneración del archivo y un `pjsip reload` en Asterisk.
* Asterisk se convertía en un punto único de registración, impidiendo cualquier escalamiento horizontal: en un cluster con N Asterisks habría que replicar y sincronizar el mismo conf en cada uno, y los softphones quedaban "atados" a la instancia donde estuvieran registrados.

En la nueva versión, los softphones de los agentes se registran directamente contra **Kamailio**, que asume el rol de *SIP registrar* y outbound proxy. Asterisk deja de conocer a los agentes individualmente: ya no existe `oml_pjsip_agents.conf`. Cuando el motor ARI necesita contactar a un agente, origina un canal hacia un endpoint genérico cuyo `outbound_proxy` apunta a Kamailio, y este último resuelve el contacto real del agente desde su tabla de localización (`usrloc`).

Beneficios:
* **Cero configuración estática por agente en Asterisk:** desaparecen tanto el archivo como los reloads asociados a operaciones CRUD de usuarios.
* **Desacople registrar/medios:** Asterisk pasa a ser un motor de medios anónimo. La identidad y autenticación SIP del agente vive únicamente en Kamailio.
* **Escalabilidad horizontal real:** múltiples instancias Asterisk pueden compartir la misma base de agentes registrados en Kamailio sin replicación de configuración. Kamailio enruta cada `INVITE` a la instancia Asterisk apropiada o a cualquiera disponible.
* **Failover transparente:** si una instancia Asterisk cae, Kamailio simplemente redirige el tráfico a otra; el softphone del agente nunca se entera del cambio porque sigue registrado contra el mismo Kamailio.
* **Aislamiento y superficie de ataque reducida:** Asterisk queda detrás del SBC, sin exponer su puerto SIP a los softphones; las políticas de rate-limit, blacklisting y autenticación se concentran en Kamailio.

### Escalabilidad Horizontal: Salida de `app_queues.so`
El ACD anterior se apoyaba sobre `app_queues.so` (la aplicación `Queue()` nativa de Asterisk) como mecanismo central de encolamiento y distribución. Si bien este módulo es robusto, impone dos restricciones estructurales:
1. **Estado en la memoria local del proceso Asterisk:** las colas, los miembros, las posiciones de los llamantes y las estadísticas viven exclusivamente en la RAM de un único proceso. Una llamada ingresada al nodo Asterisk-A no puede ser atendida por un agente registrado en el nodo Asterisk-B, porque ninguno de los dos comparte la cola.
2. **Coordinación vía AMI:** mantener vista coherente del estado obligaba a buses AMI con eventos sincrónicos, generando *race conditions* y un cuello de botella en el plano de control bajo carga elevada.

Al sustituir `app_queues.so` por una capa de encolamiento implementada en Python sobre **Redis** (consumida por el motor ARI), el ACD se vuelve intrínsecamente *cluster-aware*:
* **Cola global compartida:** todas las instancias Asterisk leen y escriben sobre las mismas estructuras en Redis. La membresía de agentes, la posición de los llamantes en cola, las estadísticas y los SLAs son un único conjunto distribuido.
* **Routing cross-node:** una llamada que entra por Asterisk-A puede ser puenteada (vía Kamailio + ARI) hacia un agente cuyo softphone está conectado a Asterisk-B. La selección del agente ocurre en el plano lógico (Python/Redis), no en el plano de medios.
* **Algoritmos de distribución sobre el cluster completo:** estrategias como *least-recent-call*, *skills-based routing* o *round-robin* se evalúan sobre el universo total de agentes loggeados, no por instancia. Esto elimina el sub-aprovechamiento típico de clusters federados con colas locales.
* **Add/Remove de nodos en caliente:** sumar o retirar un nodo Asterisk no requiere migrar colas ni miembros; basta con que el nuevo nodo apunte al mismo Redis/ARI compartido y se registre en el pool de Kamailio.
* **Resiliencia:** la caída de un nodo Asterisk no destruye el estado de las colas (que vive en Redis). Las llamadas en curso en ese nodo se pierden, pero las colas siguen operando y el resto del cluster absorbe la carga.

En conjunto con el desacople de endpoints vía Kamailio, esto rompe el modelo monolítico histórico ("un Asterisk = un ACD") y habilita una topología donde los nodos Asterisk son motores de medios *stateless* e intercambiables, escalables horizontalmente según la demanda de canales concurrentes.

### Workers de Logs
La dependencia de `res_odbc` (que forzaba a Asterisk a escribir directamente en la base de datos de manera bloqueante) ha sido removida. En su lugar, se implementan **Workers de Logs** dedicados (`source/workers/logger.py`). Los eventos del ciclo de vida de la llamada se envían a través de colas de mensajes y son procesados asincrónicamente por un worker en Python. Este worker se encarga de compilar los CDRs (Call Detail Records) y la trazabilidad del agente, ejecutando las inserciones en PostgreSQL. Esto desacopla las operaciones I/O de la base de datos del rendimiento en tiempo real de Asterisk.

## Impacto y Consideraciones para Despliegue

### Para QA (Quality Assurance)
* **Testing de Concurrencia:** Prestar especial atención a pruebas de estrés y concurrencia elevada en campañas, ya que el motor de distribución es completamente nuevo.
* **Flujos de Transferencia:** Validar exhaustivamente las transferencias ciegas y consultivas, dado el rediseño usando manipulación de *bridges* por ARI.
* **Sincronización de Estados:** Comprobar que los agentes en pausa/deslogueo reflejan sus estados instantáneamente y no reciben llamadas no deseadas, verificando que Redis opere sin "race conditions".
* **VoiceBots:** Ejecutar planes de prueba híbridos donde humanos y VoiceBots gestionen interacciones simultáneas sobre la misma campaña.
* **Registración de Agentes contra Kamailio:** Validar el ciclo completo de login/logout de softphones (registración, *re-register*, expiración) verificando que la `usrloc` de Kamailio sea la única fuente de contacto. Probar escenarios de pérdida de red del softphone y reconexión.
* **Pruebas Multi-Nodo:** En ambientes con más de un nodo Asterisk, validar que una llamada entrante a Asterisk-A pueda ser efectivamente atendida por un agente registrado a través de Asterisk-B, y que la cola en Redis se mantenga consistente.

### Para DevOps
* **Migraciones de Base de Datos:** Es obligatorio correr migraciones tras el despliegue para aplicar los cambios en los modelos de agentes (soporte de VoiceBots) y modificaciones del esquema de logs.
* **Variables de Entorno:** Aparecen nuevos componentes (motor ARI en Python y workers asíncronos). Asegurarse de inyectar correctamente las credenciales de conexión a Redis y a la API de ARI en el PBX.
* **Dependencias de Infraestructura:** Kamailio y RTPengine ahora asumen un rol crítico y obligatorio, no solo para el manejo de NAT sino también como **registrar SIP** de los softphones de agentes. Dimensionar Kamailio considerando la cantidad total de registraciones concurrentes y la tasa de `REGISTER`.
* **Limpieza de Configuraciones Legacy:** Numerosos archivos `.conf` han sido deprecados y eliminados, entre ellos `oml_extensions_precall.conf`, `oml_manager.conf` y especialmente **`oml_pjsip_agents.conf`** (los endpoints de agente ya no se generan en Asterisk). Revisar scripts de automatización/Ansible para evitar que intenten generarlos o inyectarlos.
* **Topología Cluster-Ready:** Al eliminarse `app_queues.so` y la registración estática de agentes en Asterisk, el despliegue puede escalar horizontalmente sumando nodos Asterisk al pool detrás de Kamailio, todos compartiendo la misma instancia Redis/ARI. Planificar el dimensionamiento de Redis (alta disponibilidad, persistencia) ya que se vuelve componente crítico del estado del cluster.
