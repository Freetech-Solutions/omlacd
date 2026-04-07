"""
Cliente HTTP para notificar al backend Django eventos desde el ACD.

Permite enviar notificaciones al agente vía el endpoint notify_call_blocked
cuando una llamada manual es bloqueada por validación de ruta.
"""

import logging
from typing import Any, Optional

import requests

from config import settings

logger = logging.getLogger(__name__)

NOTIFY_CALL_BLOCKED_PATH = "/api/v1/asterisk/notify_call_blocked/"
DEFAULT_REASON = (
    "La campaña no tiene ruta saliente configurada o el número no cumple con los patrones de discado configurados."
)
REQUEST_TIMEOUT_SECONDS = 5


def notify_call_blocked(
    agent_id: Any,
    phone_number: str,
    campaign_id: Any,
    reason: Optional[str] = None,
) -> None:
    """
    Notifica al backend que una llamada manual fue bloqueada (ej. por falta de ruta saliente).

    El backend envía la notificación al stream del agente vía Channels.
    Errores de red o HTTP se loguean pero no se propagan.

    Args:
        agent_id: ID del agente.
        phone_number: Número que se intentó marcar.
        campaign_id: ID de la campaña.
        reason: Motivo del bloqueo; si no se pasa, se usa DEFAULT_REASON.
    """
    base = f"{settings.OMNILEADS_PROTOCOL}://{settings.OMNILEADS_HOSTNAME}"
    url = f"{base.rstrip('/')}{NOTIFY_CALL_BLOCKED_PATH}"
    payload = {
        "agent_id": agent_id,
        "phone_number": str(phone_number),
        "campaign_id": campaign_id,
        "reason": reason if reason is not None else DEFAULT_REASON,
    }
    try:
        response = requests.post(
            url,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
            verify=settings.OMNILEADS_VERIFY_SSL,
        )
        response.raise_for_status()
        logger.debug(
            "notify_call_blocked sent successfully for agent_id=%s, phone_number=%s",
            agent_id,
            phone_number,
        )
    except requests.exceptions.Timeout:
        logger.warning(
            "notify_call_blocked timeout for agent_id=%s, url=%s",
            agent_id,
            url,
        )
    except requests.exceptions.RequestException as e:
        logger.warning(
            "notify_call_blocked failed for agent_id=%s: %s",
            agent_id,
            e,
            exc_info=False,
        )
