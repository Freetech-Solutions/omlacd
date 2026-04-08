import logging
import time
import threading
from datetime import datetime
from typing import Optional, Dict

from ari_manager import ARI
from state import CallRegistry, CallContext, ConsultationData
from config import settings
from services.agent_status_service import AgentStatusService
from constants import CallType
from state_helpers import is_channel_in_context
from utils import build_oml_sip_headers

class TransferManager:
    """
    Gestor de transferencias refactorizado para arquitectura distribuida.
    Usa Redis (CallRegistry) como fuente de verdad y bloqueos distribuidos.
    
    Garantías de Concurrencia:
    - Thread-safe: Utiliza locks distribuidos de Redis a través de `state_store.lock()`
      para proteger operaciones críticas sobre el estado de las llamadas
    - Los métodos que modifican el estado de transferencia (ej: `blind_to_endpoint()`)
      deben adquirir el lock distribuido antes de leer/modificar el contexto
    - Las operaciones ARI (originate, hangup) se realizan fuera del lock para evitar
      bloqueos prolongados, pero se verifica el estado nuevamente antes de operaciones
      críticas irreversibles
    - IMPORTANTE: Los métodos públicos que modifican estado deben usar locks distribuidos
      para garantizar atomicidad en entornos multi-instancia
    
    VENTANAS DE TIEMPO CRÍTICAS IDENTIFICADAS:
    
    Este documento identifica todas las ventanas de tiempo críticas (race conditions potenciales)
    en los métodos de transferencia. Cada método documenta sus ventanas específicas en su docstring.
    
    Resumen de ventanas críticas por método:
    
    1. blind_to_endpoint():
       - Entre liberar lock inicial y verificar antes de originate (MEDIO)
       - Entre verificar estado y ejecutar originate (BAJO)
       - Entre originate exitoso y reporte (BAJO)
       - Entre excepción y rollback completo del estado (BAJO)
    
    2. blind_to_campaign():
       - Entre liberar lock y ejecutar operaciones ARI (MEDIO)
       - Entre operaciones ARI y reporte (BAJO)
    
    3. consult_start():
       - Entre crear bridge y actualizar contexto (MEDIO)
       - Entre mover agente y verificar antes de originate (MEDIO)
       - Entre originate y guardar canal en contexto (MEDIO)
    
    4. consult_complete():
       - Entre leer estado y ejecutar operaciones ARI (MEDIO)
       - Entre completar operaciones ARI y actualizar estado final (MEDIO)
    
    5. consult_cancel():
       - Entre leer estado y ejecutar operaciones ARI de rollback (MEDIO)
       - Entre completar operaciones ARI y limpiar estado (MEDIO)
    
    6. on_transfer_leg_start():
       - Búsqueda de contexto sin lock (MEDIO)
       - Entre actualizar estado y agregar canal al bridge (MEDIO)
       - Entre agregar canal y marcar transferencia completada (BAJO)
       - Entre fallo de operación ARI y rollback del estado (BAJO)
       - Entre completar transferencia y actualizar estado del agente (BAJO)
       - Entre completar transferencia y colgar agente anterior (BAJO)
    
    7. on_consult_leg_start():
       - Entre leer consult_bridge_id y agregar canal al bridge (MEDIO)
    
    Estrategias de mitigación comunes:
    - Verificación de estado dentro del lock antes de operaciones críticas
    - Copia de valores necesarios dentro del lock antes de liberarlo
    - Mantenimiento de flags de estado (transfer_in_progress) hasta completar operaciones físicas
    - Rollback de operaciones en caso de error o cancelación concurrente
    - Verificación post-operación para detectar cambios de estado concurrentes
    """

    def __init__(self, state_store: CallRegistry, ari_client: ARI, reporter, agent_status_service: Optional[AgentStatusService] = None):
        self.state_store = state_store
        self.ari = ari_client
        self.reporter = reporter
        self.agent_status_service = agent_status_service
        # Logger estandarizado por módulo
        self.logger = logging.getLogger(__name__)
        self.asterisk_app = settings.ARI_APP
        # Node ID para reportes
        self.node_id = settings.NODE_ID

    # ----------------- Helpers -----------------
    def _get_context(self, call_id: str) -> Optional[CallContext]:
        """Helper seguro para obtener contexto."""
        if not call_id:
            return None
        return self.state_store.get(call_id)

    def _verify_state_integrity(
        self,
        ctx: Optional[CallContext],
        expected_bridge_id: Optional[str] = None,
        expected_transfer_in_progress: Optional[bool] = None,
    ) -> bool:
        """
        Verifica la integridad del estado tras operaciones de I/O (ARI) para detectar
        condiciones de carrera (ej. cliente colgó mientras se ejecutaba originate).

        Debe invocarse dentro del lock, justo después de re-adquirirlo tras I/O.

        Returns:
            True si el estado es válido para continuar; False si hay que abortar.
        """
        if ctx is None:
            return False
        if getattr(ctx, "call_ended", False):
            return False
        if expected_bridge_id is not None:
            if not ctx.bridge_id or ctx.bridge_id != expected_bridge_id:
                return False
        if expected_transfer_in_progress is not None:
            if ctx.transfer_in_progress != expected_transfer_in_progress:
                return False
        return True

    def _safe_int(self, val):
        try:
            return int(val)
        except (ValueError, TypeError):
            return 0

    def _build_transfer_headers(
        self, 
        ctx: Optional[CallContext] = None, 
        target_agent_id=None,
        call_id: Optional[str] = None,
        id_customer: Optional[int] = None,
        id_camp: Optional[int] = None,
        phone_number: Optional[str] = None,
        call_type: Optional[int] = None,
        uniqueid_pstn: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Reconstruye headers SIP desde el modelo Pydantic CallContext o valores individuales.
        
        Args:
            ctx: Contexto de llamada (opcional, se usa si se proporciona)
            target_agent_id: ID del agente destino
            call_id: ID de la llamada (se usa si ctx no se proporciona)
            id_customer: ID del cliente (se usa si ctx no se proporciona)
            id_camp: ID de la campaña (se usa si ctx no se proporciona)
            phone_number: Número de teléfono (se usa si ctx no se proporciona)
            call_type: Tipo de llamada (se usa si ctx no se proporciona)
            uniqueid_pstn: UniqueID PSTN (se usa si ctx no se proporciona)
        """
        # Si se proporciona ctx, extraer valores de ahí; si no, usar valores individuales
        if ctx:
            _call_id = ctx.call_id
            _id_customer = ctx.id_customer
            _id_camp = ctx.id_camp
            _phone_number = ctx.phone_number
            _call_type = ctx.call_type
            _uniqueid_pstn = ctx.uniqueid_pstn
        else:
            _call_id = call_id or ""
            _id_customer = id_customer
            _id_camp = id_camp
            _phone_number = phone_number
            _call_type = call_type
            _uniqueid_pstn = uniqueid_pstn
        
        # Reutilizar helper centralizado para construir headers y variables legacy.
        # Para transferencias usamos:
        # - origin="transfer"
        # - include_legacy_vars=True (mantiene compatibilidad OMLUNIQUEID, etc.)
        # - include_branch_id=True para X-OML-BranchID y X-OML-ExternalTelNumber
        headers = build_oml_sip_headers(
            call_id=_call_id,
            customer_id=_id_customer,
            camp_id=_id_camp,
            phone_number=_phone_number,
            call_type=_call_type,
            agent_id=target_agent_id,
            origin="transfer",
            asterisk_id=_uniqueid_pstn,
            include_legacy_vars=True,
            include_branch_id=True,
        )

        return headers

    def _start_moh(self, bridge_id: Optional[str]) -> None:
        if not bridge_id: return
        try:
            self.ari.post(f"bridges/{bridge_id}/moh")
        except Exception:
            pass

    def _originate_transfer_leg(
        self,
        endpoint: str,
        bridge_id: str,
        cust_ch: str,
        ctx: Optional[CallContext] = None,
        target_agent_id: Optional[int] = None,
        call_id: Optional[str] = None,
        id_customer: Optional[int] = None,
        id_camp: Optional[int] = None,
        phone_number: Optional[str] = None,
        call_type: Optional[int] = None,
        uniqueid_pstn: Optional[str] = None,
    ) -> str:
        """
        Origina la nueva pierna de transferencia.
        
        Args:
            endpoint: Endpoint destino de la transferencia
            bridge_id: ID del bridge donde se agregará la nueva pierna
            cust_ch: Normalmente es call_id (preferido) para búsqueda estable,
                    pero puede ser channel_id para compatibilidad con código legacy.
                    El valor se pasa como 'customer_id' en appArgs para que
                    on_transfer_leg_start() pueda recuperar el contexto.
            ctx: Contexto de llamada (opcional, se usa si se proporciona)
            target_agent_id: ID del agente destino
            call_id: ID de la llamada (se usa si ctx no se proporciona)
            id_customer: ID del cliente (se usa si ctx no se proporciona)
            id_camp: ID de la campaña (se usa si ctx no se proporciona)
            phone_number: Número de teléfono (se usa si ctx no se proporciona)
            call_type: Tipo de llamada (se usa si ctx no se proporciona)
            uniqueid_pstn: UniqueID PSTN (se usa si ctx no se proporciona)
        """
        # Si se proporciona ctx, extraer valores de ahí; si no, usar valores individuales
        if ctx:
            _call_id = ctx.call_id
            _id_customer = ctx.id_customer
            _id_camp = ctx.id_camp
            _phone_number = ctx.phone_number
            _call_type = ctx.call_type
            _uniqueid_pstn = ctx.uniqueid_pstn
        else:
            _call_id = call_id or ""
            _id_customer = id_customer
            _id_camp = id_camp
            _phone_number = phone_number
            _call_type = call_type
            _uniqueid_pstn = uniqueid_pstn
        
        # Args clave: 'transfer_target:true' avisa al Router que es una pierna especial
        # customer_id: Se pasa como call_id (preferido) o channel_id (fallback).
        #              on_transfer_leg_start() buscará primero por call_id y luego por canal.
        app_args = f"bridge_id:{bridge_id},is_agent:false,transfer_target:true,customer_id:{cust_ch}"

        variables = self._build_transfer_headers(
            ctx=ctx,
            target_agent_id=target_agent_id,
            call_id=_call_id,
            id_customer=_id_customer,
            id_camp=_id_camp,
            phone_number=_phone_number,
            call_type=_call_type,
            uniqueid_pstn=_uniqueid_pstn
        )
        variables["PJSIP_HEADER(add,X-Transfer-From)"] = str(_uniqueid_pstn or _call_id)

        caller_id_str = f"Transfer: {_phone_number}" if _phone_number else "Transfer"

        self.logger.info(f"📞 Originando transferencia a {endpoint} para {_call_id}")

        result = self.ari.originate_channel_op(
            endpoint=endpoint,
            app=self.asterisk_app,
            appArgs=app_args,
            callerId=caller_id_str,
            timeout=settings.TRANSFER_TIMEOUT,
            variables=variables,
        )

        if not result.get("ok"):
            error_msg = result.get("error") or f"Originate devolvió respuesta inválida para {endpoint}"
            self.logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)

        data = result.get("data") or {}
        return data.get("id")

    def _log_transfer_result(
        self, 
        ctx: Optional[CallContext] = None, 
        initiator: str = "", 
        transfer_type: str = "", 
        endpoint: str = "", 
        result: str = "", 
        duration_ms: int = 0, 
        leg_unique_id: str = None, 
        sip_reason: str = None, 
        target_agent_id: int = None, 
        target_camp_id: int = None,
        call_id: Optional[str] = None,
        id_customer: Optional[int] = None,
        id_camp: Optional[int] = None,
        call_type: Optional[int] = None,
        agent_id: Optional[int] = None
    ):
        """
        Wrapper para reportar transferencias.
        
        Args:
            ctx: Contexto de llamada (opcional, se usa si se proporciona)
            initiator: Iniciador de la transferencia
            transfer_type: Tipo de transferencia
            endpoint: Endpoint destino
            result: Resultado de la transferencia
            duration_ms: Duración en milisegundos
            leg_unique_id: ID único de la pierna
            sip_reason: Razón SIP si hay error
            target_agent_id: ID del agente destino
            target_camp_id: ID de la campaña destino
            call_id: ID de la llamada (se usa si ctx no se proporciona)
            id_customer: ID del cliente (se usa si ctx no se proporciona)
            id_camp: ID de la campaña (se usa si ctx no se proporciona)
            call_type: Tipo de llamada (se usa si ctx no se proporciona)
            agent_id: ID del agente origen (se usa si ctx no se proporciona)
        """
        # Si se proporciona ctx, extraer valores de ahí; si no, usar valores individuales
        if ctx:
            _call_id = ctx.call_id
            _id_customer = ctx.id_customer
            _id_camp = ctx.id_camp
            _call_type = ctx.call_type
            _agent_id = ctx.agent_id
        else:
            _call_id = call_id or ""
            _id_customer = id_customer
            _id_camp = id_camp
            _call_type = call_type
            _agent_id = agent_id
        
        self.reporter.log_transfer(
            call_id=_call_id, # Usar el ID de negocio principal
            contacto_id=_id_customer,
            campana_id_origen=_id_camp,
            tipo_llamada=_call_type,
            agente_origen_id=_agent_id or 0,
            initiated_by=initiator,
            transfer_type=transfer_type,
            numero_extra=endpoint,
            target_agent_id=target_agent_id,
            target_campaign_id=target_camp_id,
            leg_unique_id=leg_unique_id,
            resultado=result,
            duration_ms=duration_ms,
            sip_reason=sip_reason,
            node_id=self.node_id
        )

    def _get_target_agent_id_with_fallback(self, channel_id: str, target_agent_id: Optional[int] = None) -> Optional[int]:
        """
        Obtiene el target_agent_id desde el parámetro proporcionado o desde las variables del canal como fallback.
        
        Estrategia:
        1. Primero intenta usar el target_agent_id proporcionado como parámetro
        2. Si no está disponible, intenta obtener desde la variable OMLAGENTID del canal
        
        Args:
            channel_id: ID del canal del agente destino
            target_agent_id: ID del agente destino (opcional, se usa si se proporciona)
            
        Returns:
            ID del agente destino si se encuentra, None en caso contrario
        """
        # 1. Usar el target_agent_id proporcionado si está disponible (método preferido)
        if target_agent_id:
            self.logger.debug(
                f"_get_target_agent_id_with_fallback: target_agent_id obtenido desde parámetro: {target_agent_id}"
            )
            return target_agent_id
        
        # 2. Fallback: intentar obtener desde variables del canal
        if channel_id:
            try:
                oml_agent_id = self.ari.get_channel_variable(channel_id, "OMLAGENTID")
                if oml_agent_id:
                    try:
                        target_agent_id = int(oml_agent_id)
                        self.logger.info(
                            f"_get_target_agent_id_with_fallback: target_agent_id obtenido desde variable "
                            f"OMLAGENTID del canal {channel_id}: {target_agent_id}"
                        )
                        return target_agent_id
                    except (ValueError, TypeError):
                        self.logger.warning(
                            f"_get_target_agent_id_with_fallback: OMLAGENTID no es un entero válido: {oml_agent_id}"
                        )
            except Exception as e:
                self.logger.debug(
                    f"_get_target_agent_id_with_fallback: No se pudo obtener OMLAGENTID desde variables "
                    f"del canal {channel_id}: {e}"
                )
        
        return None

    def _extract_agent_id_safe(self, agent_channel_id: str) -> Optional[str]:
        """
        Extrae el ID del agente transferidor desde el canal (Agente A).

        Prioridad:
        1. Variable de canal X-OML-AgentID (inyectada durante el Dial).
        2. Variable legacy OMLAGENTID.
        3. Fallback: nombre del canal (ej. PJSIP/1004-00000a -> 1004) o CallerID.

        Returns:
            ID del agente como string si se encuentra, None en caso contrario.
        """
        if not agent_channel_id:
            return None
        # 1. Variable de canal X-OML-AgentID
        try:
            value = self.ari.get_channel_variable(agent_channel_id, "X-OML-AgentID")
            if value is not None and str(value).strip():
                return str(value).strip()
        except Exception:
            pass
        # 2. Variable legacy OMLAGENTID
        try:
            value = self.ari.get_channel_variable(agent_channel_id, "OMLAGENTID")
            if value is not None and str(value).strip():
                return str(value).strip()
        except Exception:
            pass
        # 3. Fallback: nombre del canal (PJSIP/1004-00000a -> 1004) o CallerID
        try:
            ch = self.ari.get_channel_details(agent_channel_id)
            if not ch or not isinstance(ch, dict):
                return None
            name = ch.get("name") or ""
            if name:
                # Formato típico: PJSIP/1004-00000a o SIP/1004-00000a -> extraer 1004
                if "/" in name and "-" in name:
                    part = name.split("/", 1)[-1].split("-", 1)[0]
                    if part.isdigit():
                        return part
                elif "/" in name:
                    part = name.split("/", 1)[-1]
                    if part.isdigit():
                        return part
            caller = ch.get("caller") or {}
            raw = caller.get("number") or caller.get("name") or ""
            if raw and isinstance(raw, str):
                parts = [p for p in raw.split("_") if p]
                if len(parts) >= 4:
                    return parts[-1]
        except Exception:
            pass
        return None

    def _update_agent_to_oncall(self, agent_id, call_id, bridge_id, campaign_id, contact_number):
        """
        Actualiza el estado del agente a ONCALL con todos los campos.
        
        Args:
            agent_id: ID del agente a actualizar
            call_id: ID de la llamada
            bridge_id: ID del bridge de la llamada
            campaign_id: ID de la campaña
            contact_number: Número de contacto
        """
        if not self.agent_status_service:
            self.logger.warning(
                f"_update_agent_to_oncall: agent_status_service no está disponible "
                f"para agente {agent_id}"
            )
            return
        
        if not agent_id:
            self.logger.warning("_update_agent_to_oncall: agent_id vacío")
            return
        
        try:
            self.agent_status_service.set_oncall(
                agent_id=agent_id,
                call_id=call_id,
                bridge_id=bridge_id,
                campaign_id=campaign_id,
                contact_number=contact_number
            )
            self.logger.info(
                f"✅ Estado del agente {agent_id} actualizado a ONCALL "
                f"para llamada {call_id}"
            )
        except Exception as e:
            self.logger.error(
                f"❌ Error actualizando estado del agente {agent_id} a ONCALL: {e}",
                exc_info=True
            )

    # -------------------------------------------------------------------------
    # 1) Blind Transfer -> Endpoint
    # -------------------------------------------------------------------------
    def blind_to_agent(
        self,
        call_id: str,
        target_agent_id: int,
        agente_id: Optional[int] = None
    ) -> bool:
        """
        Transferencia ciega a otro agente.
        Resuelve el SIP del agente y llama a blind_to_endpoint.
        """
        if not self.agent_status_service:
            self.logger.error("BlindToAgent: agent_status_service no está disponible")
            return False
        
        sip_agent = self.agent_status_service.get_sip(target_agent_id)
        if not sip_agent:
            self.logger.error(f"BlindToAgent: No se encontró SIP para agente {target_agent_id}")
            return False
            
        # Construir endpoint PJSIP
        # Asumimos que settings está disponible y tiene WEBRTC_TRUNK
        webrtc_trunk = settings.WEBRTC_TRUNK
        endpoint = f"PJSIP/{sip_agent}@{webrtc_trunk}"
        
        self.logger.info(f"🔄 BlindToAgent: {call_id} -> Agente {target_agent_id} ({endpoint})")
        
        return self.blind_to_endpoint(
            unique_id=call_id,
            endpoint=endpoint,
            agente_id=agente_id,
            transfer_type="AGENT",
            target_agent_id=target_agent_id
        )

    def blind_to_endpoint(
        self,
        unique_id: str, # Puede ser call_id o uniqueid
        endpoint: str,
        agente_id: Optional[int] = None,
        transfer_type: str = "EXTENSION",
        target_agent_id: Optional[int] = None,
    ) -> bool:
        """
        Realiza una transferencia ciega a un endpoint.
        
        Thread-safety:
            Este método utiliza locks distribuidos de Redis para garantizar atomicidad.
            Adquiere el lock antes de leer/modificar el contexto, y lo libera antes de
            realizar operaciones ARI. Verifica el estado nuevamente antes de operaciones
            críticas irreversibles (como originate) para prevenir race conditions.
        
        VENTANAS DE TIEMPO CRÍTICAS:
        1. Líneas 510-528: Entre liberar el lock inicial y verificar antes de originate.
           - Riesgo: Otro thread puede cancelar la transferencia (marcar transfer_in_progress=False)
             o modificar el contexto durante las operaciones ARI (start_moh, hangup).
           - Mitigación: Se verifica el estado nuevamente dentro del lock antes de originate (línea 528).
           - Impacto: MEDIO - Si se cancela, el originate no se ejecuta (correcto), pero las operaciones
             ARI previas (start_moh, hangup) ya se ejecutaron (pueden ser reversibles).
        
        2. Líneas 538-552: Entre verificar estado y ejecutar originate.
           - Riesgo: El estado puede cambiar entre la verificación (línea 530) y el originate (línea 552),
             aunque los valores críticos ya fueron copiados dentro del lock.
           - Mitigación: Los valores necesarios se copian dentro del lock (líneas 540-547) antes de liberarlo.
           - Impacto: BAJO - Los valores copiados garantizan consistencia para el originate.
        
        3. Líneas 563-580: Entre originate exitoso y reporte de éxito.
           - Riesgo: Si falla el reporte o hay una excepción no capturada, el estado puede quedar inconsistente.
           - Mitigación: El flag transfer_in_progress se mantiene hasta que on_transfer_leg_start lo limpie,
             o se hace rollback en el bloque except (línea 585).
           - Impacto: BAJO - El rollback maneja correctamente los errores.
        
        4. Líneas 582-622: Entre excepción y rollback completo del estado.
           - Riesgo: Si ocurre una excepción durante originate o reporte, hay una ventana donde el estado
             puede quedar inconsistente (transfer_in_progress=True pero la transferencia falló) antes del rollback.
           - Mitigación: El rollback se ejecuta inmediatamente dentro de un lock, limpiando transfer_in_progress
             y reportando el error (líneas 585-622).
           - Impacto: BAJO - El rollback es rápido y restaura el estado correctamente.
        """
        
        inicio_ts = time.monotonic()

        # 1. BLOQUEO DISTRIBUIDO (Race Condition Protection)
        # Adquirir lock y mantener estado consistente dentro del lock.
        # En este primer bloque solo marcamos la transferencia como "en progreso"
        # y persistimos el contexto, pero no ejecutamos todavía operaciones ARI.
        with self.state_store.lock(unique_id):
            # Recargar contexto dentro del lock por seguridad
            ctx = self.state_store.get(unique_id)
            if not ctx or ctx.transfer_in_progress:
                self.logger.warning(f"⛔ Transferencia bloqueada/concurrente para {unique_id}")
                return False
            
            # Leer todos los valores necesarios dentro del lock para mantener consistencia
            initiator = "VOICEBOT" if ctx.is_voicebot else "AGENTE"
            bridge_id = ctx.bridge_id
            agent_channel = ctx.agent_channel
            call_id = ctx.call_id
            agent_id = ctx.agent_id

            # SAFETY NET: Asegurar que sabemos quién transfiere (Agente A) antes de colgar su canal
            if ctx and not ctx.agent_id and agent_channel:
                try:
                    agent_id_str = self._extract_agent_id_safe(agent_channel)
                    if agent_id_str:
                        self.logger.info(
                            f"BlindTransfer: Forzando agent_id={agent_id_str} en contexto para {unique_id}"
                        )
                        ctx.agent_id = int(agent_id_str)
                        self.state_store.register_unsafe(unique_id, ctx)
                        agent_id = ctx.agent_id
                except Exception as e:
                    self.logger.warning(
                        f"No se pudo preservar agent_id en transferencia ciega: {e}"
                    )
            
            # Marcar inicio de transferencia
            ctx.transfer_in_progress = True
            # Guardar target_agent_id en el contexto para recuperarlo en on_transfer_leg_start()
            if target_agent_id is not None:
                ctx.target_agent_id = target_agent_id
            self.state_store.register_unsafe(unique_id, ctx)

        try:
            # 2. Verificar estado nuevamente dentro del lock ANTES de ejecutar operaciones ARI.
            #    Esto minimiza la ventana donde podríamos hacer MOH/hangup sobre una llamada
            #    cuya transferencia ya fue cancelada o modificada por otro thread.
            with self.state_store.lock(unique_id):
                ctx = self.state_store.get(unique_id)
                if not ctx or not ctx.transfer_in_progress:
                    # Otra transferencia canceló o completó, abortar
                    self.logger.warning(f"⛔ Transferencia cancelada antes de ejecutar operaciones ARI para {unique_id}")
                    # Limpiar flag si aún está marcado
                    if ctx and ctx.transfer_in_progress:
                        ctx.transfer_in_progress = False
                        self.state_store.register_unsafe(unique_id, ctx)
                    return False
                # Copiar todos los valores necesarios dentro del lock para garantizar consistencia.
                # No mantener referencia al objeto ctx fuera del lock.
                bridge_id = ctx.bridge_id
                call_id = ctx.call_id
                id_customer = ctx.id_customer
                id_camp = ctx.id_camp
                phone_number = ctx.phone_number
                call_type = ctx.call_type
                uniqueid_pstn = ctx.uniqueid_pstn
                agent_id = ctx.agent_id

            # 3. Operaciones ARI fuera del lock para evitar bloqueos prolongados.
            #    En este punto ya hemos verificado que la transferencia sigue activa.
            self._start_moh(bridge_id)

            # Colgar al agente actual si existe
            if agent_channel:
                # hangup_channel ya tiene retry logic, si retorna False significa que falló después de reintentos
                hangup_result = self.ari.hangup_channel(agent_channel)
                if not hangup_result:
                    self.logger.warning(
                        f"⚠️ No se pudo colgar canal de agente {agent_channel} después de reintentos. "
                        f"Continuando con la transferencia."
                    )
                # TODO: Aquí podrías liberar al agente en Redis si tienes lógica de presencia
            
            # 4. Originar llamada (operación crítica irreversible)
            #    NOTA: originate se ejecuta fuera del lock para no bloquear, pero ya verificamos
            #    el estado dentro del lock y tenemos los valores copiados (no el objeto ctx)
            leg_id = self._originate_transfer_leg(
                endpoint=endpoint,
                bridge_id=bridge_id,
                cust_ch=call_id,
                target_agent_id=target_agent_id,
                call_id=call_id,
                id_customer=id_customer,
                id_camp=id_camp,
                phone_number=phone_number,
                call_type=call_type,
                uniqueid_pstn=uniqueid_pstn
            )

            # SAFETY CHECK: Re-verificar estado tras I/O (cliente pudo colgar, bridge cambiar, etc.)
            with self.state_store.lock(unique_id):
                ctx = self.state_store.get(unique_id)
                if not self._verify_state_integrity(
                    ctx,
                    expected_bridge_id=bridge_id,
                    expected_transfer_in_progress=True,
                ):
                    self.logger.warning(
                        f"Transfer abortada: Estado inestable tras I/O en {unique_id}"
                    )
                    if leg_id:
                        try:
                            self.ari.hangup_channel(leg_id)
                        except Exception:
                            pass
                    return False

            # Reportar Éxito
            self._log_transfer_result(
                initiator=initiator, 
                transfer_type=transfer_type, 
                endpoint=endpoint, 
                result="OK", 
                duration_ms=int((time.monotonic()-inicio_ts)*1000),
                leg_unique_id=leg_id, 
                target_agent_id=target_agent_id,
                call_id=call_id,
                id_customer=id_customer,
                id_camp=id_camp,
                call_type=call_type,
                agent_id=agent_id
            )
            return True

        except Exception as e:
            self.logger.error(f"❌ Falló Blind Transfer: {e}")
            # Rollback del flag
            with self.state_store.lock(unique_id):
                ctx = self.state_store.get(unique_id)
                if ctx:
                    ctx.transfer_in_progress = False
                    self.state_store.register_unsafe(unique_id, ctx)
            
            # Reportar Error
            # Recargar valores dentro del lock para el reporte de error
            with self.state_store.lock(unique_id):
                ctx = self.state_store.get(unique_id)
                if ctx:
                    call_id = ctx.call_id
                    id_customer = ctx.id_customer
                    id_camp = ctx.id_camp
                    call_type = ctx.call_type
                    agent_id = ctx.agent_id
                else:
                    # Si no hay contexto, usar valores por defecto
                    call_id = unique_id
                    id_customer = None
                    id_camp = None
                    call_type = None
                    agent_id = None
            
            self._log_transfer_result(
                initiator=initiator, 
                transfer_type=transfer_type, 
                endpoint=endpoint, 
                result="FAILED", 
                duration_ms=int((time.monotonic()-inicio_ts)*1000),
                sip_reason=str(e),
                call_id=call_id,
                id_customer=id_customer,
                id_camp=id_camp,
                call_type=call_type,
                agent_id=agent_id
            )
            return False

    # -------------------------------------------------------------------------
    # 2) Blind Transfer -> Campaña (Re-encolar)
    # -------------------------------------------------------------------------
    def blind_to_campaign(self, unique_id: str, target_camp_id: str, extra_headers: Optional[dict] = None) -> bool:
        """
        Saca al agente y vuelve a encolar al cliente.
        
        VENTANAS DE TIEMPO CRÍTICAS:
        1. Entre leer y modificar el contexto y ejecutar operaciones ARI (MOH/hangup).
           - Mitigación: Mantener el estado lógico lo más cercano posible al estado físico y
             usar un lock adicional posterior a las operaciones ARI para validar que la
             transferencia sigue activa antes de consolidar cambios definitivos.
        """
        # 1. Primer bloque protegido: validar y marcar que hay una transferencia en curso,
        #    pero sin desmontar todavía el estado lógico de la llamada.
        with self.state_store.lock(unique_id):
            ctx = self.state_store.get(unique_id)
            if not ctx or ctx.transfer_in_progress:
                return False
            
            # Guardar canal actual del agente para operar físicamente fuera del lock
            old_agent_ch = ctx.agent_channel

            # Marcar que hay una transferencia en curso para evitar operaciones concurrentes
            ctx.transfer_in_progress = True  # Se limpiará cuando entre a cola o se asigne
            ctx.is_transferred = True

            # Guardar headers extra si vienen en la petición
            if extra_headers:
                ctx.custom_sip_headers = extra_headers

            # Persistir cambio mínimo de estado
            self.state_store.register_unsafe(unique_id, ctx)
            
            # Copiar valores necesarios dentro del lock - no mantener referencia al objeto ctx
            bridge_id = ctx.bridge_id
            call_id = ctx.call_id
            id_customer = ctx.id_customer
            source_camp_id = ctx.id_camp
            call_type = ctx.call_type
            source_agent_id = ctx.agent_id
        
        # 2. Ejecución física fuera del lock (no modifica estado compartido)
        self._start_moh(bridge_id)
        
        if old_agent_ch:
            # hangup_channel ya tiene retry logic, si retorna False significa que falló después de reintentos
            hangup_result = self.ari.hangup_channel(old_agent_ch)
            if not hangup_result:
                self.logger.warning(
                    f"⚠️ No se pudo colgar canal de agente anterior {old_agent_ch} después de reintentos. "
                    f"Continuando con la transferencia a campaña."
                )
        
        # 3. Segundo bloque protegido: consolidar cambios lógicos solo si la transferencia
        #    sigue activa después de las operaciones físicas.
        with self.state_store.lock(unique_id):
            ctx = self.state_store.get(unique_id)

            # SAFETY CHECK: Re-verificar estado tras I/O (cliente pudo colgar, bridge cambiar, etc.)
            if not self._verify_state_integrity(
                ctx,
                expected_bridge_id=bridge_id,
                expected_transfer_in_progress=True,
            ):
                self.logger.warning(
                    f"Transfer abortada: Estado inestable tras I/O en {unique_id}"
                )
                return False

            # Actualizar estado para la nueva campaña de forma atómica
            ctx.id_camp = int(target_camp_id)
            ctx.agent_id = None
            old_agent_ch = ctx.agent_channel
            ctx.agent_channel = None
            # transfer_in_progress permanece en True hasta que el flujo de re-encolado
            # complete y la llamada vuelva a asignarse
            ctx.transfer_count += 1

            self.state_store.register_unsafe(unique_id, ctx)

        # 4. Reporte (fuera del lock, usando copias locales)
        self._log_transfer_result(
            ctx=None,
            initiator="AGENTE",
            transfer_type="QUEUE",
            endpoint=f"CAMPAIGN:{target_camp_id}",
            result="OK",
            target_camp_id=int(target_camp_id),
            call_id=call_id,
            id_customer=id_customer,
            id_camp=source_camp_id,
            call_type=call_type,
            agent_id=source_agent_id
        )

        # TODO: Aquí debes disparar el evento de redistribución.
        # En la arquitectura nueva, esto podría ser publicar en Redis o llamar al Router.
        # self.router.redistribute(ctx) 
        self.logger.info(f"🔄 Cliente re-encolado en campaña {target_camp_id}. Esperando distribución...")
        
        return True

    # -------------------------------------------------------------------------
    # 3) Consultative Transfer
    # -------------------------------------------------------------------------
    def consult_start(self, unique_id: str, target_endpoint: str, target_agent_id: Optional[int]) -> bool:
        """
        Fase 1: Consulta. Crea bridge temporal.
        
        VENTANAS DE TIEMPO CRÍTICAS:
        1. Líneas 767-772: Entre crear el bridge de consulta y actualizar el contexto.
           - Riesgo: El bridge existe en Asterisk pero no está registrado en el contexto. Si otro thread
             intenta cancelar la consulta, no sabrá del bridge y no podrá limpiarlo correctamente.
           - Mitigación: Se verifica dentro del lock si la consulta fue cancelada (línea 774) y se destruye
             el bridge si es necesario (línea 777).
           - Impacto: MEDIO - Puede dejar bridges huérfanos si hay una cancelación concurrente.
        
        2. Líneas 787-792: Entre mover el agente al bridge de consulta y verificar antes de originate.
           - Riesgo: El agente ya fue movido físicamente al bridge de consulta, pero si la consulta se
             cancela entre estas operaciones, el rollback debe moverlo de vuelta (línea 798).
           - Mitigación: Se verifica el estado dentro del lock antes de originate (línea 793) y se hace
             rollback si es necesario.
           - Impacto: MEDIO - El rollback restaura el estado, pero hay una ventana donde el agente está
             en el bridge incorrecto.
        
        3. Líneas 829-847: Entre originate y guardar el canal en el contexto.
           - Riesgo: El canal de la pierna de consulta existe pero no está registrado en el contexto.
             Si otro thread intenta cancelar, no sabrá del canal y no podrá colgarlo.
           - Mitigación: Se guarda el canal dentro del lock inmediatamente después del originate (línea 851).
           - Impacto: MEDIO - Puede dejar canales huérfanos si hay una cancelación concurrente justo después
             del originate.
        """
        
        # 1. Inicializar consulta dentro del lock para mantener estado consistente
        with self.state_store.lock(unique_id):
            ctx = self.state_store.get(unique_id)
            if not ctx or not ctx.bridge_id or not ctx.agent_channel:
                return False
            
            ctx.transfer_in_progress = True
            
            # Inicializar estructura de consulta en Pydantic
            ctx.consultation = ConsultationData(
                active=True,
                initiator_agent_ch=ctx.agent_channel,
                main_bridge=ctx.bridge_id,
                target_agent_id=target_agent_id,
                target_endpoint=target_endpoint
            )
            self.state_store.register_unsafe(unique_id, ctx)
            # Leer todos los valores necesarios dentro del lock para mantener consistencia
            main_bridge_id = ctx.bridge_id
            agent_channel = ctx.agent_channel

        try:
            # 2. Crear Bridge Consulta (operación ARI fuera del lock)
            consult_bridge = self.ari.create_bridge(bridge_type="mixing")
            consult_bridge_id = consult_bridge['id']
            
            # 3. Actualizar ID del bridge de consulta dentro del lock
            with self.state_store.lock(unique_id):
                ctx = self.state_store.get(unique_id)
                if not ctx or not ctx.consultation:
                    # La consulta fue cancelada, destruir bridge creado
                    try:
                        self.ari.destroy_bridge(consult_bridge_id)
                    except Exception:
                        pass
                    return False
                ctx.consultation.consult_bridge = consult_bridge_id
                self.state_store.register_unsafe(unique_id, ctx)
                # Recargar valores dentro del lock para garantizar consistencia
                main_bridge_id = ctx.bridge_id
                agent_channel = ctx.agent_channel

            # 4. Operaciones ARI fuera del lock (no modifican estado compartido)
            self._start_moh(main_bridge_id)
            self.ari.remove_channel_from_bridge(main_bridge_id, agent_channel)
            self.ari.add_channel_to_bridge(consult_bridge_id, agent_channel)

            # 5. Verificar estado nuevamente dentro del lock antes de operación crítica (originate)
            with self.state_store.lock(unique_id):
                ctx = self.state_store.get(unique_id)
                if not ctx or not ctx.consultation or not ctx.consultation.active:
                    # La consulta fue cancelada, limpiar recursos
                    try:
                        self.ari.add_channel_to_bridge(main_bridge_id, agent_channel)
                        self.ari.destroy_bridge(consult_bridge_id)
                    except Exception:
                        pass
                    return False
                # Recargar valores dentro del lock para garantizar consistencia antes de originate
                # Copiar todos los valores necesarios dentro del lock - no mantener referencia al objeto ctx
                phone_number = ctx.phone_number
                uniqueid_pstn = ctx.uniqueid_pstn
                call_id = ctx.call_id
                id_customer = ctx.id_customer
                id_camp = ctx.id_camp
                call_type = ctx.call_type

            # 6. Llamar al Agente B (Target) - operación crítica irreversible
            # NOTA: originate se ejecuta fuera del lock para no bloquear, pero ya verificamos
            # el estado dentro del lock y tenemos los valores copiados (no el objeto ctx)
            app_args = f"bridge_id:{consult_bridge_id},is_agent:false,consult_leg:true,customer_id:{unique_id}"
            
            variables = self._build_transfer_headers(
                ctx=None,
                target_agent_id=target_agent_id,
                call_id=call_id,
                id_customer=id_customer,
                id_camp=id_camp,
                phone_number=phone_number,
                call_type=call_type,
                uniqueid_pstn=uniqueid_pstn
            )
            variables["PJSIP_HEADER(add,X-Consult-From)"] = str(uniqueid_pstn or call_id)

            result = self.ari.originate_channel_op(
                endpoint=target_endpoint,
                app=self.asterisk_app,
                appArgs=app_args,
                callerId=f"Consult: {phone_number}",
                timeout=settings.CONSULT_TIMEOUT,
                variables=variables,
            )
            
            if not result.get("ok"):
                self.logger.error(
                    f"❌ Error crítico al originar pierna de consulta hacia {target_endpoint} "
                    f"después de reintentos para {unique_id}: {result.get('error')}"
                )
                return False
            
            data = result.get("data") or {}
            consult_leg_id = data.get("id")
            if not consult_leg_id:
                self.logger.error(
                    f"❌ Respuesta de ARI sin 'id' al originar pierna de consulta hacia {target_endpoint} "
                    f"para {unique_id}: {data}"
                )
                return False
            
            # 7. Guardar el canal de la pierna de consulta dentro del lock
            with self.state_store.lock(unique_id):
                ctx = self.state_store.get(unique_id)
                if ctx and ctx.consultation:
                    ctx.consultation.consult_leg_ch = consult_leg_id
                    self.state_store.register_unsafe(unique_id, ctx)
            return True

        except Exception as e:
            self.logger.error(f"❌ Error ConsultStart: {e}")
            # Rollback: intentar cancelar la consulta
            try:
                self.consult_cancel(unique_id)
            except Exception:
                pass
            return False

    def three_way_add(self, call_id: str, bridge_id: str, target_agent_id: int) -> bool:
        """
        Añade un tercer participante (agente) a la llamada existente (conferencia 3-way).
        Origina hacia el agente; cuando conteste, el Router (StasisStart three_way_leg)
        añadirá su canal al bridge existente.
        """
        if not self.agent_status_service:
            self.logger.error("three_way_add: agent_status_service not available")
            return False
        sip_agent = self.agent_status_service.get_sip(str(target_agent_id))
        if not sip_agent:
            self.logger.error(f"three_way_add: Could not resolve SIP for agent {target_agent_id}")
            return False
        webrtc_trunk = settings.WEBRTC_TRUNK
        target_endpoint = f"PJSIP/{sip_agent}@{webrtc_trunk}"
        app_args = f"three_way_leg:true,bridge_id:{bridge_id},customer_id:{call_id}"

        self.logger.info(
            f"three_way_add: Originating to agent {target_agent_id} (endpoint={target_endpoint}) "
            f"for call_id={call_id}, bridge_id={bridge_id}"
        )
        result = self.ari.originate_channel_op(
            endpoint=target_endpoint,
            app=self.asterisk_app,
            appArgs=app_args,
            callerId=f"3-way: {call_id}",
            timeout=settings.DEFAULT_ORIGINATE_TIMEOUT,
        )
        if not result.get("ok"):
            self.logger.error(
                f"❌ three_way_add: Failed to originate to agent {target_agent_id}: {result.get('error')}"
            )
            return False
        data = result.get("data") or {}
        channel_id = data.get("id")
        if channel_id:
            self.logger.info(f"three_way_add: Channel {channel_id} ringing for agent {target_agent_id}")
        return True

    def three_way_conf_add(self, call_id: str, bridge_id: str, sip_number: str) -> bool:
        """
        Añade un tercer participante a la llamada existente (conferencia 3-way).
        Origina hacia sip_number (PJSIP/{sip_number}@{WEBRTC_TRUNK}); cuando conteste,
        el Router (StasisStart three_way_conf_leg) añadirá el canal al bridge existente
        y lo registrará en other_channels. Al colgar la llamada se limpian ese canal,
        el agente y la pierna PSTN.
        """
        webrtc_trunk = settings.WEBRTC_TRUNK
        target_endpoint = f"PJSIP/{sip_number}@{webrtc_trunk}"
        app_args = f"three_way_conf_leg:true,bridge_id:{bridge_id},customer_id:{call_id}"

        self.logger.info(
            f"three_way_conf_add: Originating to sip_number={sip_number} (endpoint={target_endpoint}) "
            f"for call_id={call_id}, bridge_id={bridge_id}"
        )
        result = self.ari.originate_channel_op(
            endpoint=target_endpoint,
            app=self.asterisk_app,
            appArgs=app_args,
            callerId=f"3-way-conf: {call_id}",
            timeout=settings.DEFAULT_ORIGINATE_TIMEOUT,
        )
        if not result.get("ok"):
            self.logger.error(
                f"❌ three_way_conf_add: Failed to originate to sip_number={sip_number}: {result.get('error')}"
            )
            return False
        data = result.get("data") or {}
        channel_id = data.get("id")
        if channel_id:
            self.logger.info(f"three_way_conf_add: Channel {channel_id} ringing for sip_number={sip_number}")
        return True

    def consult_complete(self, unique_id: str) -> bool:
        """
        Fase 2: Completar transferencia.
        
        VENTANAS DE TIEMPO CRÍTICAS:
        1. Líneas 898-923: Entre leer el estado y ejecutar operaciones ARI.
           - Riesgo: El estado puede cambiar durante las operaciones ARI. Por ejemplo, otro thread podría
             cancelar la consulta (consult_cancel) mientras se ejecutan las operaciones físicas.
           - Mitigación: Se verifica el estado nuevamente dentro del lock antes de actualizar el estado final
             (línea 926), pero las operaciones ARI ya se ejecutaron.
           - Impacto: MEDIO - Si se cancela concurrentemente, las operaciones ARI (mover canal, hangup,
             destruir bridge) pueden ejecutarse incluso si la consulta fue cancelada, causando conflictos.
        
        2. Líneas 923-940: Entre completar operaciones ARI y actualizar el estado final.
           - Riesgo: Las operaciones físicas se completaron pero el estado aún no refleja la transferencia
             completada. Otros threads pueden leer un estado donde la consulta sigue activa pero físicamente
             ya se completó.
           - Mitigación: Se verifica que la consulta sigue activa antes de actualizar (línea 928).
           - Impacto: MEDIO - Puede causar inconsistencias si otro thread intenta cancelar durante esta ventana.
        """
        # 1. Leer estado inicial dentro del lock
        with self.state_store.lock(unique_id):
            ctx = self.state_store.get(unique_id)
            if not ctx or not ctx.consultation or not ctx.consultation.active:
                return False
            
            cons = ctx.consultation
            # Leer todos los valores necesarios dentro del lock para mantener consistencia
            consult_leg_ch = cons.consult_leg_ch
            main_bridge = cons.main_bridge
            initiator_agent_ch = cons.initiator_agent_ch
            consult_bridge = cons.consult_bridge
            target_agent_id = cons.target_agent_id
            # Marcar que el próximo hangup del agente iniciador proviene de una transferencia consultativa
            # para que la lógica genérica de finalización de llamada lo ignore y no cuelgue al PSTN.
            try:
                ctx.ignore_next_agent_hangup = True
            except AttributeError:
                # Compatibilidad defensiva por si el atributo aún no existe en versiones antiguas del modelo
                self.logger.warning(
                    "ConsultComplete: CallContext no tiene atributo ignore_next_agent_hangup; "
                    "el hangup del agente iniciador podría gatillar cierre completo de llamada."
                )
            else:
                self.logger.info(
                    f"ConsultComplete: Marcando ignore_next_agent_hangup=True para call_id={unique_id} "
                    f"(iniciador={initiator_agent_ch})"
                )
                self.state_store.register_unsafe(unique_id, ctx)
            
        try:
            # 2. Operaciones ARI fuera del lock (no modifican estado compartido)
            # Mover Agente B al Main Bridge
            if consult_leg_ch:
                self.ari.add_channel_to_bridge(main_bridge, consult_leg_ch)
            
            # Colgar Agente A
            # hangup_channel ya tiene retry logic
            hangup_result = self.ari.hangup_channel(initiator_agent_ch)
            if not hangup_result:
                self.logger.warning(
                    f"⚠️ No se pudo colgar canal de agente iniciador {initiator_agent_ch} "
                    f"después de reintentos. Continuando con la transferencia."
                )
            
            # Limpiar MOH
            try:
                self.ari.delete(f"bridges/{main_bridge}/moh")
            except Exception: pass
            
            # Destruir bridge temporal
            if consult_bridge:
                try:
                    self.ari.destroy_bridge(consult_bridge)
                except Exception: pass

            # 3. Actualizar Estado Final en Redis dentro del lock
            # Verificar que la consulta sigue activa antes de actualizar estado final
            with self.state_store.lock(unique_id):
                ctx = self.state_store.get(unique_id)
                if not ctx or not ctx.consultation or not ctx.consultation.active:
                    # La consulta fue cancelada, no actualizar estado
                    self.logger.warning(f"ConsultComplete: Consulta ya no está activa para {unique_id}")
                    return False
                
                # Obtener TODOS los valores necesarios del contexto dentro del lock antes de actualizarlo
                # Esto garantiza que usamos valores consistentes del estado anterior a la actualización
                call_id = ctx.call_id
                id_customer = ctx.id_customer
                id_camp = ctx.id_camp
                call_type = ctx.call_type
                agent_id = ctx.agent_id  # Agente original antes de la transferencia
                target_agent_id = ctx.consultation.target_agent_id
                endpoint = ctx.consultation.target_endpoint or ""
                leg_unique_id = ctx.consultation.consult_leg_ch
                
                # Actualizar estado después de obtener todos los valores necesarios
                # El nuevo agente es el B
                ctx.agent_channel = leg_unique_id
                ctx.agent_id = target_agent_id
                ctx.consultation = None # Limpiar objeto consulta
                ctx.transfer_in_progress = False
                ctx.is_transferred = True
                ctx.transfer_count += 1
                self.state_store.register_unsafe(unique_id, ctx)

            # 4. Registrar resultado de la transferencia
            # Todos los valores ya fueron obtenidos dentro del lock antes de liberarlo
            self._log_transfer_result(
                ctx=None,
                initiator="AGENTE",
                transfer_type="AGENT",
                endpoint=endpoint,
                result="OK",
                duration_ms=0,  # La duración puede calcularse si se guarda timestamp de inicio
                leg_unique_id=leg_unique_id,
                target_agent_id=target_agent_id,
                call_id=call_id,
                id_customer=id_customer,
                id_camp=id_camp,
                call_type=call_type,
                agent_id=agent_id
            )

            return True

        except Exception as e:
            self.logger.error(f"❌ Error ConsultComplete: {e}")
            return False

    def consult_cancel(self, unique_id: str) -> bool:
        """
        Fase 3: Cancelar consulta. Revierte cambios y restaura estado inicial.
        
        VENTANAS DE TIEMPO CRÍTICAS:
        1. Líneas 982-1014: Entre leer el estado y ejecutar operaciones ARI de rollback.
           - Riesgo: El estado puede cambiar durante las operaciones ARI. Por ejemplo, otro thread podría
             completar la consulta (consult_complete) mientras se ejecutan las operaciones de rollback.
           - Mitigación: Se verifica el estado nuevamente dentro del lock antes de limpiar (línea 1016),
             pero las operaciones ARI ya se ejecutaron.
           - Impacto: MEDIO - Si se completa concurrentemente, las operaciones de rollback (mover canal,
             hangup, destruir bridge) pueden ejecutarse incluso si la consulta ya se completó, causando
             conflictos con las operaciones de consult_complete.
        
        2. Líneas 1014-1024: Entre completar operaciones ARI y limpiar el estado.
           - Riesgo: Las operaciones físicas de rollback se completaron pero el estado aún no se limpió.
             Otros threads pueden leer un estado donde la consulta sigue activa pero físicamente ya se canceló.
           - Mitigación: Se verifica que la consulta sigue activa antes de limpiar (línea 1021).
           - Impacto: MEDIO - Puede causar inconsistencias si otro thread intenta completar durante esta ventana.
        """
        # 1. Leer estado inicial dentro del lock
        with self.state_store.lock(unique_id):
            ctx = self.state_store.get(unique_id)
            if not ctx or not ctx.consultation or not ctx.consultation.active:
                self.logger.warning(f"ConsultCancel: No hay consulta activa para {unique_id}")
                return False
            
            cons = ctx.consultation
            # Leer todos los valores necesarios dentro del lock para mantener consistencia
            initiator_agent_ch = cons.initiator_agent_ch
            main_bridge = cons.main_bridge
            consult_leg_ch = cons.consult_leg_ch
            consult_bridge = cons.consult_bridge
            
        try:
            # 2. Operaciones ARI fuera del lock (no modifican estado compartido)
            # Mover Agente A de vuelta al bridge principal
            if initiator_agent_ch and main_bridge:
                try:
                    self.ari.add_channel_to_bridge(main_bridge, initiator_agent_ch)
                except Exception as e:
                    self.logger.error(f"Error moviendo agente A al bridge principal: {e}")
            
            # Colgar el canal del Agente B (si existe)
            if consult_leg_ch:
                # hangup_channel ya tiene retry logic, si retorna False significa que falló después de reintentos
                hangup_result = self.ari.hangup_channel(consult_leg_ch)
                if not hangup_result:
                    self.logger.warning(
                        f"⚠️ No se pudo colgar canal de consulta {consult_leg_ch} después de reintentos. "
                        f"Continuando con la cancelación."
                    )
            
            # Detener MOH
            if main_bridge:
                try:
                    self.ari.delete(f"bridges/{main_bridge}/moh")
                except Exception:
                    pass
            
            # Destruir bridge de consulta
            if consult_bridge:
                try:
                    self.ari.destroy_bridge(consult_bridge)
                except Exception as e:
                    self.logger.error(f"Error destruyendo bridge de consulta: {e}")
            
            # 3. Limpiar estado dentro del lock
            with self.state_store.lock(unique_id):
                ctx = self.state_store.get(unique_id)
                if ctx and ctx.consultation:
                    # Verificar que la consulta sigue activa antes de limpiar
                    # Recargar dentro del lock para garantizar consistencia
                    if ctx.consultation.active:
                        ctx.consultation = None
                        ctx.transfer_in_progress = False
                        self.state_store.register_unsafe(unique_id, ctx)
            
            self.logger.info(f"✅ Consulta cancelada para {unique_id}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Error ConsultCancel: {e}")
            return False

    # -------------------------------------------------------------------------
    # 4) Event Handlers (Called by Router)
    # -------------------------------------------------------------------------
    def on_transfer_target_hangup(self, channel_id: str) -> None:
        """
        Maneja el hangup del agente destino en una transferencia ciega.

        Escenario principal:
        - Llamada manual A ↔ PSTN
        - BlindToAgent a B (TransferLegStart completa, ctx.is_transferred=True)
        - Agente B (canal actual del agente) cuelga

        Regla de negocio:
        - Cuando el último agente del bridge cuelga en una transferencia ciega,
          debemos colgar también el leg PSTN y limpiar el bridge para que el
          cliente no quede "aislado" en el bridge.

        Seguridad / concurrencia:
        - Usa búsquedas por índice de canal para localizar el contexto.
        - Revalida el contexto dentro de un lock distribuido antes de actuar.
        - Ejecuta operaciones ARI fuera del lock para evitar bloqueos prolongados.
        """
        if not channel_id:
            return

        try:
            # 1) Localizar contexto inicial por canal (sin lock, solo para obtener call_id)
            ctx = self.state_store.get_by_channel(channel_id)
            if not ctx or ctx.type.value != CallType.MANUAL.value:
                # No es una llamada manual o el canal no pertenece a una llamada conocida
                self.logger.debug(
                    f"TransferTargetHangup: Canal {channel_id} no pertenece a llamada manual conocida; "
                    f"no se aplica política de transferencia ciega"
                )
                return

            call_id = ctx.call_id
            if not call_id:
                return

            # 2) Revalidar contexto dentro de un lock y capturar valores necesarios
            is_transferred = False
            is_current_agent_channel = False
            pstn_channel = None
            bridge_id = None

            with self.state_store.lock(call_id):
                fresh_ctx = self.state_store.get(call_id)
                if not fresh_ctx:
                    return

                # Verificar que el canal siga perteneciendo a este contexto usando helper común
                if not is_channel_in_context(fresh_ctx, channel_id):
                    # El canal ya no pertenece a esta llamada; evitar actuar con contexto obsoleto
                    self.logger.debug(
                        "TransferTargetHangup: Canal %s ya no está asociado a call_id=%s; "
                        "ignorando hangup para evitar usar contexto obsoleto",
                        channel_id,
                        call_id,
                    )
                    return

                # Separar explícitamente el canal de agente actual del canal del agente iniciador.
                current_agent_ch = fresh_ctx.agent_channel
                initiator_agent_ch = getattr(fresh_ctx, "uniqueid_agent", None)

                # Flag para transferencias consultivas: ignorar el próximo hangup del agente iniciador.
                ignore_next_agent_hangup = getattr(fresh_ctx, "ignore_next_agent_hangup", False)
                is_initiator_agent_channel = bool(initiator_agent_ch and channel_id == initiator_agent_ch)

                # Si el hangup proviene del agente iniciador en una transferencia consultiva
                # y el flag indica que debemos ignorarlo, no tocar PSTN ni bridge.
                if ignore_next_agent_hangup and is_initiator_agent_channel:
                    self.logger.info(
                        "TransferTargetHangup: Ignorando hangup del agente iniciador en "
                        f"transferencia consultiva (ignore_next_agent_hangup=True, call_id={fresh_ctx.call_id}, "
                        f"channel_id={channel_id})"
                    )
                    try:
                        # Consumir el flag para que solo aplique al próximo hangup del iniciador.
                        fresh_ctx.ignore_next_agent_hangup = False
                        self.state_store.register_unsafe(call_id, fresh_ctx)
                    except Exception:
                        # Ser defensivos: si no podemos persistir el cambio, al menos no aplicar
                        # la política de TransferTargetHangup en este hangup.
                        self.logger.debug(
                            "TransferTargetHangup: No se pudo actualizar ignore_next_agent_hangup "
                            f"para call_id={call_id}. El flag podría seguir activo."
                        )
                    return

                is_transferred = getattr(fresh_ctx, "is_transferred", False)
                # A partir de aquí, solo consideramos agent_channel como canal de agente actual.
                is_current_agent_channel = current_agent_ch == channel_id

                # Solo aplicar la regla cuando ya se completó una transferencia
                # y el hangup proviene del canal de agente "activo" en el contexto.
                if is_transferred and is_current_agent_channel:
                    pstn_channel = fresh_ctx.pstn_channel or fresh_ctx.uniqueid_pstn
                    bridge_id = fresh_ctx.bridge_id
                    self.logger.info(
                        f"📞 TransferTargetHangup: Detectado hangup de agente destino "
                        f"para call_id={fresh_ctx.call_id}, channel_id={channel_id}. "
                        f"pstn_channel={pstn_channel}, bridge_id={bridge_id}"
                    )
                else:
                    # No es un escenario de transferencia ciega con agente destino,
                    # no aplicar lógica especial.
                    self.logger.debug(
                        f"TransferTargetHangup: Hangup en canal {channel_id} para call_id={fresh_ctx.call_id} "
                        f"no cumple condiciones de transferencia ciega "
                        f"(is_transferred={is_transferred}, is_current_agent_channel={is_current_agent_channel}); "
                        f"no se colgará PSTN ni se destruirá bridge"
                    )
                    return

            # 3) Ejecutar operaciones ARI fuera del lock
            # 3.1 Colgar leg PSTN si existe
            if pstn_channel and pstn_channel.strip():
                try:
                    hangup_result = self.ari.hangup_channel(pstn_channel)
                    if hangup_result:
                        self.logger.info(
                            f"✅ TransferTargetHangup: PSTN leg {pstn_channel} colgado "
                            f"tras hangup de agente destino {channel_id} (call_id={call_id})"
                        )
                    else:
                        # Puede retornar False si el canal ya no existe (404)
                        self.logger.debug(
                            f"⚠️ TransferTargetHangup: hangup_channel retornó False para "
                            f"PSTN leg {pstn_channel} (puede que ya no exista) "
                            f"(call_id={call_id})"
                        )
                except Exception as e:
                    self.logger.error(
                        f"❌ TransferTargetHangup: Error al colgar PSTN leg {pstn_channel} "
                        f"para call_id={call_id}: {e}",
                        exc_info=True,
                    )

            # 3.2 Destruir bridge si hay identificador disponible
            if bridge_id and bridge_id.strip():
                try:
                    destroy_result = self.ari.destroy_bridge(bridge_id)
                    if destroy_result:
                        self.logger.info(
                            f"✅ TransferTargetHangup: Bridge {bridge_id} destruido "
                            f"tras hangup de agente destino (call_id={call_id})"
                        )
                    else:
                        self.logger.debug(
                            f"⚠️ TransferTargetHangup: destroy_bridge retornó False para "
                            f"bridge {bridge_id} (puede que ya no exista) "
                            f"(call_id={call_id})"
                        )
                except Exception as e:
                    self.logger.error(
                        f"❌ TransferTargetHangup: Error crítico al destruir bridge {bridge_id} "
                        f"para call_id={call_id}: {e}",
                        exc_info=True,
                    )

        except Exception as e:
            # No propagar excepción al router; solo loggear para debugging.
            self.logger.error(
                f"❌ TransferTargetHangup: Error general manejando hangup de canal {channel_id}: {e}",
                exc_info=True,
            )

    def on_transfer_leg_start(self, channel_id: str, args: Dict[str, str]) -> None:
        """
        Maneja el inicio de la nueva pierna en una Blind Transfer.
        Trigger: StasisStart con transfer_target:true
        
        Args:
            args: Debe contener 'customer_id' que normalmente es call_id (preferido),
                 pero puede ser channel_id para compatibilidad. Este valor se pasa
                 desde _originate_transfer_leg() donde cust_ch normalmente es ctx.call_id.
        
        Estrategia de búsqueda robusta (en orden de preferencia):
        1. Búsqueda directa por call_id (más eficiente y estable)
        2. Búsqueda por canal usando índice secundario
        3. Extracción de call_id desde variables del canal (OMLUNIQUEID)
        4. Búsqueda por bridge_id si está disponible en args
        
        VENTANAS DE TIEMPO CRÍTICAS:
        1. Líneas 1083-1120: Búsqueda de contexto sin lock.
           - Riesgo: Múltiples threads pueden buscar el contexto simultáneamente usando diferentes estrategias.
             El contexto puede cambiar durante la búsqueda (por ejemplo, si se elimina o modifica).
           - Mitigación: Se recarga el contexto dentro del lock después de encontrarlo (línea 1124).
           - Impacto: MEDIO - Si el contexto se elimina durante la búsqueda, se detecta al recargar dentro
             del lock. Sin embargo, hay una ventana donde se puede usar un contexto obsoleto.
        
        2. Líneas 1153-1161: Entre actualizar el estado y agregar el canal al bridge.
           - Riesgo: El estado ya refleja el nuevo agent_channel pero el canal aún no está físicamente
             en el bridge. Otros threads pueden leer un estado donde agent_channel apunta a un canal
             que no está en el bridge.
           - Mitigación: Se mantiene transfer_in_progress=True hasta que la operación ARI sea exitosa
             (línea 1141), reduciendo la ventana de inconsistencia.
           - Impacto: MEDIO - Otros threads pueden ver agent_channel actualizado antes de que el canal
             esté en el bridge, pero el flag transfer_in_progress indica que la operación aún está en progreso.
        
        3. Líneas 1161-1172: Entre agregar el canal al bridge y marcar transfer_in_progress=False.
           - Riesgo: El canal ya está físicamente en el bridge pero el estado aún indica transfer_in_progress=True.
             Otros threads pueden intentar iniciar una nueva transferencia pensando que la anterior aún está
             en progreso, aunque físicamente ya se completó.
           - Mitigación: Se verifica el estado dentro del lock antes de marcar como completada (línea 1170),
             asegurando que el estado no haya cambiado durante la operación ARI.
           - Impacto: BAJO - La ventana es corta y se verifica el estado antes de actualizar.
        
        4. Líneas 1180-1202: Entre fallo de operación ARI y rollback del estado.
           - Riesgo: Si falla add_channel_to_bridge, hay una ventana donde el estado puede quedar inconsistente
             (agent_channel actualizado pero el canal no está en el bridge) antes de que se ejecute el rollback.
           - Mitigación: El rollback se ejecuta inmediatamente dentro de un lock, restaurando agent_channel
             y verificando que el estado no haya cambiado (línea 1189).
           - Impacto: BAJO - El rollback es rápido y restaura el estado correctamente.
        
        5. Líneas 1204-1236: Entre completar la transferencia y actualizar el estado del agente.
           - Riesgo: La transferencia está completa pero el estado del agente aún no se actualiza. Si el
             canal no está "Up" aún, el estado se actualizará más tarde, pero hay una ventana donde el
             agente puede no estar marcado como ONCALL aunque la transferencia se completó.
           - Mitigación: Se verifica el estado del canal y se actualiza inmediatamente si está "Up", o se
             espera al evento ChannelStateChange.
           - Impacto: BAJO - El estado se actualiza tan pronto como el canal está disponible.
        
        6. Líneas 1243-1251: Entre completar la transferencia y colgar el agente anterior.
           - Riesgo: El nuevo agente ya está en el bridge pero el agente anterior aún no se ha colgado.
             Durante esta ventana, ambos agentes pueden estar técnicamente asociados con la llamada.
           - Mitigación: El hangup se ejecuta inmediatamente después de completar la transferencia. El flag
             transfer_in_progress ya está en False, indicando que la transferencia se completó.
           - Impacto: BAJO - La ventana es corta y el hangup es una operación rápida.
        """
        customer_id = args.get('customer_id')
        if not customer_id:
            self.logger.error(f"❌ TransferLegStart: No se recibió customer_id para canal {channel_id}")
            return # No podemos vincular

        # Estrategia de búsqueda robusta (sin usar get_all()):
        # Extraer call_id inmediatamente después de encontrar el contexto para minimizar
        # la ventana de tiempo donde el contexto puede cambiar antes de adquirir el lock.
        call_id = None
        
        # 1. Primero intentar buscar por call_id directamente (más eficiente y estable)
        ctx = self.state_store.get(customer_id)
        if ctx:
            call_id = ctx.call_id
            self.logger.debug(f"TransferLegStart: Contexto encontrado por call_id directo: {customer_id}")
        else:
            # 2. Si no encuentra, intentar buscar por canal usando índice secundario
            ctx = self.state_store.get_by_channel(customer_id)
            if ctx:
                call_id = ctx.call_id
                self.logger.debug(f"TransferLegStart: Contexto encontrado por índice de canal: {customer_id}")
        
        # 3. Si aún no encuentra, intentar extraer call_id desde variables del canal
        if not call_id:
            try:
                # Intentar obtener call_id desde variable OMLUNIQUEID del canal
                oml_uniqueid = self.ari.get_channel_variable(channel_id, "OMLUNIQUEID")
                if oml_uniqueid:
                    ctx = self.state_store.get(oml_uniqueid)
                    if ctx:
                        call_id = ctx.call_id
                        self.logger.debug(f"TransferLegStart: Contexto encontrado por OMLUNIQUEID del canal: {oml_uniqueid}")
            except Exception as e:
                self.logger.debug(f"TransferLegStart: No se pudo obtener OMLUNIQUEID del canal {channel_id}: {e}")
        
        # 4. Si aún no encuentra, intentar buscar por bridge_id si está disponible
        if not call_id:
            bridge_id = args.get('bridge_id')
            if bridge_id:
                ctx = self.state_store.get_by_bridge_id(bridge_id)
                if ctx:
                    call_id = ctx.call_id
                    self.logger.debug(f"TransferLegStart: Contexto encontrado por bridge_id: {bridge_id}")
        
        if not call_id:
            self.logger.error(
                f"❌ TransferLegStart: Contexto no encontrado para cliente {customer_id} "
                f"(canal: {channel_id}). Estrategias de búsqueda agotadas. "
                f"Verificar que los índices secundarios estén actualizados."
            )
            return

        # Adquirir lock usando el call_id extraído inmediatamente después de la búsqueda
        # para minimizar la ventana de tiempo donde el contexto puede cambiar
        with self.state_store.lock(call_id):
            # Recargar contexto dentro del lock para evitar usar datos obsoletos
            ctx = self.state_store.get(call_id)
            if not ctx:
                return

            self.logger.info(f"⚡ Procesando TransferLegStart para {ctx.call_id} (Nuevo Agente/Destino: {channel_id})")

            # ✅ Protección adicional: no completar transferencia sobre contextos ya terminados
            # o sin leg PSTN válido. Esto previene que una pierna de transferencia tardía
            # "reviva" una llamada que ya fue cerrada por _process_call_end() (por ejemplo,
            # por hangup del PSTN durante Ringing).
            if getattr(ctx, "call_ended", False):
                self.logger.warning(
                    "TransferLegStart: Contexto ya marcado como call_ended=True para "
                    f"call_id={ctx.call_id}. Ignorando pierna de transferencia tardía "
                    f"channel_id={channel_id}"
                )
                return

            # Para llamadas manuales, si no hay ningún identificador de leg PSTN asociado
            # consideramos que el PSTN ya no existe y no debemos completar la transferencia.
            if (
                ctx.type == CallType.MANUAL
                and not (ctx.pstn_channel or ctx.uniqueid_pstn)
            ):
                self.logger.warning(
                    "TransferLegStart: Contexto sin leg PSTN activo para llamada manual "
                    f"call_id={ctx.call_id}. Ignorando pierna de transferencia tardía "
                    f"channel_id={channel_id}"
                )
                return

            # Verificar que la transferencia sigue en progreso
            if not ctx.transfer_in_progress:
                self.logger.warning(f"TransferLegStart: Transferencia ya no está en progreso para {ctx.call_id}")
                return

            # 3. Actualizar Estado (dentro del lock para atomicidad)
            # IMPORTANTE: Mantener transfer_in_progress = True hasta que la operación ARI
            # sea exitosa para reducir la ventana de inconsistencia. Esto previene que
            # otros threads lean un estado donde agent_channel ya cambió pero la operación
            # ARI aún no se completó.
            old_agent = ctx.agent_channel
            ctx.agent_channel = channel_id  # El nuevo canal toma el lugar del agente
            # NO marcar transfer_in_progress = False todavía - se marcará después de operación ARI exitosa
            ctx.is_transferred = True
            
            # Guardar valores necesarios para usar fuera del lock
            bridge_id = ctx.bridge_id
            call_id = ctx.call_id
            campaign_id = ctx.id_camp
            phone_number = ctx.phone_number
            target_agent_id = ctx.target_agent_id  # Copiar target_agent_id dentro del lock
            
            self.state_store.register_unsafe(call_id, ctx)

        # Operaciones ARI fuera del lock para minimizar tiempo de bloqueo
        try:
            # 1. Stops MOH
            self.ari.delete(f"bridges/{bridge_id}/moh")
        except Exception: pass

        try:
            # 2. Agregar nuevo canal al Bridge (operación crítica)
            self.ari.add_channel_to_bridge(bridge_id, channel_id)
            
            # 3. Solo después de que la operación ARI sea exitosa, marcar transferencia como completada
            # Esto reduce la ventana de inconsistencia: el estado solo refleja éxito cuando
            # la operación física realmente se completó
            with self.state_store.lock(call_id):
                ctx = self.state_store.get(call_id)
                if ctx:
                    # Verificar que el estado no haya cambiado (otro thread podría haber cancelado)
                    if ctx.transfer_in_progress and ctx.agent_channel == channel_id:
                        ctx.transfer_in_progress = False
                        ctx.transfer_count += 1
                        self.state_store.register_unsafe(call_id, ctx)
                    else:
                        # El estado cambió, posiblemente cancelado por otro thread
                        self.logger.warning(
                            f"TransferLegStart: Estado cambió durante operación ARI para {call_id}. "
                            f"transfer_in_progress={ctx.transfer_in_progress}, agent_channel={ctx.agent_channel}"
                        )
                        
        except Exception as e:
            self.logger.error(f"Error agregando canal {channel_id} al bridge {bridge_id}: {e}")
            # Si falla esto, la transferencia falló. Rollback del estado
            # Como mantenemos transfer_in_progress = True, el rollback es más simple:
            # solo necesitamos restaurar agent_channel
            with self.state_store.lock(call_id):
                ctx = self.state_store.get(call_id)
                if ctx:
                    # Verificar que el estado no haya cambiado antes de hacer rollback
                    if ctx.transfer_in_progress and ctx.agent_channel == channel_id:
                        ctx.agent_channel = old_agent  # Restaurar agente anterior
                        # transfer_in_progress ya está en True, no necesita restaurarse
                        self.state_store.register_unsafe(call_id, ctx)
                        self.logger.info(
                            f"Rollback completado para {call_id}: agent_channel restaurado a {old_agent}"
                        )
                    else:
                        # El estado cambió, posiblemente manejado por otro thread
                        self.logger.warning(
                            f"Rollback omitido para {call_id}: estado ya cambió. "
                            f"transfer_in_progress={ctx.transfer_in_progress}, agent_channel={ctx.agent_channel}"
                        )
            return

        # 4. Actualizar estado del agente a ONCALL si el canal está "Up"
        # Obtener target_agent_id desde el valor copiado o desde variables del canal (fallback)
        target_agent_id = self._get_target_agent_id_with_fallback(channel_id, target_agent_id)
        
        # Verificar si el canal está "Up" y actualizar estado del agente
        if self.agent_status_service and target_agent_id:
            try:
                channel_details = self.ari.get_channel_details(channel_id)
                if channel_details and channel_details.get('state') == 'Up':
                    # El canal está "Up", actualizar inmediatamente el estado del agente a ONCALL
                    self._update_agent_to_oncall(
                        agent_id=target_agent_id,
                        call_id=call_id,
                        bridge_id=bridge_id,
                        campaign_id=campaign_id,
                        contact_number=phone_number
                    )
                    self.logger.info(
                        f"✅ TransferLegStart: Estado del agente {target_agent_id} "
                        f"actualizado a ONCALL (canal {channel_id} está Up)"
                    )
                    # Marcar agent_answered_ts para la pierna de transferencia si aún no existe
                    try:
                        with self.state_store.lock(call_id):
                            ctx = self.state_store.get(call_id)
                            if ctx and ctx.agent_channel == channel_id and not ctx.agent_answered_ts:
                                ctx.agent_answered_ts = datetime.now().isoformat()
                                self.state_store.register_unsafe(call_id, ctx)
                                self.logger.debug(
                                    f"📞 TransferLegStart: agent_answered_ts marcado para "
                                    f"call_id={call_id}, channel_id={channel_id}"
                                )
                    except Exception as ts_err:
                        self.logger.warning(
                            f"TransferLegStart: Error marcando agent_answered_ts para call_id={call_id}: {ts_err}",
                            exc_info=True
                        )
                else:
                    # El canal no está "Up" aún, el estado se actualizará cuando llegue ChannelStateChange
                    channel_state = channel_details.get('state') if channel_details else 'unknown'
                    self.logger.debug(
                        f"TransferLegStart: Canal {channel_id} en estado '{channel_state}', "
                        f"el estado del agente se actualizará cuando el canal cambie a 'Up'"
                    )
            except Exception as e:
                self.logger.warning(
                    f"TransferLegStart: Error verificando estado del canal o actualizando agente: {e}",
                    exc_info=True
                )
        elif not target_agent_id:
            self.logger.warning(
                f"TransferLegStart: No se pudo obtener target_agent_id para actualizar estado del agente "
                f"(call_id={call_id}, channel_id={channel_id})"
            )

        # 5. Colgar agente anterior si sigue vivo
        if old_agent:
            # hangup_channel ya tiene retry logic, si retorna False significa que falló después de reintentos
            hangup_result = self.ari.hangup_channel(old_agent)
            if not hangup_result:
                self.logger.warning(
                    f"⚠️ No se pudo colgar canal de agente anterior {old_agent} después de reintentos. "
                    f"Continuando con la transferencia."
                )

        self.logger.info(f"✅ Blind Transfer completada para {call_id}")


    def on_consult_leg_start(self, channel_id: str, args: Dict[str, str]) -> None:
        """
        Maneja el inicio de la pierna consultada (Agente B).
        Trigger: StasisStart con consult_leg:true
        
        VENTANAS DE TIEMPO CRÍTICAS:
        1. Líneas 1312-1317: Entre leer el consult_bridge_id y agregar el canal al bridge.
           - Riesgo: El estado puede cambiar durante la operación ARI. Por ejemplo, otro thread podría
             cancelar la consulta (consult_cancel) mientras se agrega el canal al bridge, causando que
             se agregue un canal a un bridge que está siendo destruido.
           - Mitigación: Ninguna verificación post-operación. Si la consulta se cancela concurrentemente,
             el canal puede agregarse a un bridge que luego se destruye.
           - Impacto: MEDIO - Puede causar errores en ARI si el bridge se destruye mientras se agrega
             el canal, aunque ARI maneja estos casos generalmente sin problemas.
        """
        # args tiene customer_id que es el uniqueid de la llamada principal (call_id)
        # En consult_start pasamos customer_id:{unique_id}
        
        call_id = args.get('customer_id') 
        if not call_id:
            self.logger.error(f"❌ ConsultLegStart: No se recibió customer_id (call_id)")
            return

        with self.state_store.lock(call_id):
            ctx = self.state_store.get(call_id)
            if not ctx or not ctx.consultation:
                self.logger.warning(f"ConsultLegStart: Contexto o consulta inválida para {call_id}")
                return
            
            self.logger.info(f"⚡ Procesando ConsultLegStart para {call_id}. Canal Consult: {channel_id}")

            # Guardar referencia confirmada
            ctx.consultation.consult_leg_ch = channel_id
            self.state_store.register_unsafe(call_id, ctx)
            # Guardar consult_bridge para usar fuera del lock
            consult_bridge_id = ctx.consultation.consult_bridge

        # Operación ARI fuera del lock para minimizar tiempo de bloqueo
        if consult_bridge_id:
            try:
                self.ari.add_channel_to_bridge(consult_bridge_id, channel_id)
            except Exception as e:
                self.logger.error(f"Error uniendo consult leg {channel_id} a bridge {consult_bridge_id}: {e}")
