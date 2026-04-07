from abc import ABC, abstractmethod

from ari_manager import ARI
from state import CallRegistry
from reporter import ACDReporter
from models import BaseARIEvent


class BaseHandler(ABC):
    """
    Clase base abstracta para handlers de eventos del sistema ACD modular.
    Todos los handlers deben heredar de esta clase e implementar los métodos
    abstractos para manejar los diferentes eventos del ciclo de vida de una llamada.
    """

    def __init__(
        self,
        ari_client: ARI,
        state_store: CallRegistry,
        reporter: ACDReporter
    ):
        """
        Inicializa el handler base con las dependencias necesarias.

        Args:
            ari_client: Instancia de la clase ARI para interactuar con Asterisk
            state_store: Instancia de CallRegistry para gestionar el estado de las llamadas
            reporter: Instancia de ACDReporter para reportar eventos
        """
        self.ari_client = ari_client
        self.state_store = state_store
        self.reporter = reporter

    @abstractmethod
    def on_start(self, event: BaseARIEvent) -> None:
        """
        Maneja el evento de inicio de llamada.
        Args:
            event: Evento de inicio de llamada recibido desde ARI
        """
        pass

    @abstractmethod
    def on_up(self, event: BaseARIEvent) -> None:
        """
        Maneja el evento de conexión establecida.
        Args:
            event: Evento de conexión establecida recibido desde ARI
        """
        pass

    @abstractmethod
    def on_failure(self, event: BaseARIEvent) -> None:
        """
        Maneja el evento de fallo.
        Args:
            event: Evento de fallo recibido desde ARI
        """
        pass

    def on_hangup_request(self, event: BaseARIEvent) -> None:
        """
        Maneja el evento de solicitud de colgado (ChannelHangupRequest).
        Opcional: Los handlers pueden sobrescribir este método si
        necesitan capturar la causa de colgado.
        Args:
            event: Evento ChannelHangupRequest recibido desde ARI
        """
        pass
