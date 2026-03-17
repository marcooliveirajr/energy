from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import requests
import logging

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """
    Classe base para todos os extratores.
    Define o método abstrato extract que deve ser implementado por cada subclasse.
    """
    def __init__(self, token_manager, config, credentials, spark=None):
        self.token_manager = token_manager
        self.config = config
        self.credentials = credentials
        self.spark = spark
        self.session = self._create_session()
        self.max_retries = 3
        self.retry_delay = 2
        self.timeout = 180  # segundos

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        return session

    def _get_auth_payload(self) -> Dict[str, Any]:
        return {
            "Username": self.credentials["username"],
            "Password": self.credentials["password"],
            "EnvironmentName": self.credentials["environment"]
        }

    def _make_request(self, data: Optional[Dict] = None) -> Dict[str, Any]:
        """Executa a requisição HTTP com retry em caso de timeout."""
        url = f"{self.config.base_url}{self.config.path}"
        payload = {
            "AutheticationToken": self._get_auth_payload(),
            "Data": data if data is not None else (self.config.default_data or {})
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                if self.config.method.upper() == "GET":
                    resp = self.session.get(url, params=payload, timeout=self.timeout)
                else:
                    resp = self.session.post(url, json=payload, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout tentativa {attempt}/{self.max_retries}")
                if attempt == self.max_retries:
                    raise
                import time
                time.sleep(self.retry_delay * attempt)
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro tentativa {attempt}: {e}")
                if attempt == self.max_retries:
                    raise
                time.sleep(self.retry_delay * attempt)

    @abstractmethod
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """Método abstrato – deve ser implementado por cada extrator concreto."""
        pass

class GenericExtractor(BaseExtractor):
    """
    Extrator genérico para endpoints que não precisam de lógica especial.
    """
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        try:
            response = self._make_request(kwargs.get("data"))
            if response and "Data" in response:
                data = response["Data"]
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    return [data]
            return []
        except Exception as e:
            logger.error(f"Erro no GenericExtractor: {e}")
            return []