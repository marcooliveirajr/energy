from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import requests
import logging

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    def __init__(self, token_manager, config, credentials, spark=None):
        self.token_manager = token_manager
        self.config = config
        self.credentials = credentials
        self.spark = spark
        self.session = self._create_session()
        self.max_retries = 3
        self.retry_delay = 2
        self.timeout = 180

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "MXM-Data-Pipeline/1.0"
        })
        return session

    def _get_auth_payload(self) -> Dict[str, Any]:
        return {
            "Username": self.credentials["username"],
            "Password": self.credentials["password"],
            "EnvironmentName": self.credentials["environment"]
        }

    def _make_request(self, data: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"{self.config.base_url}{self.config.path}"
        payload = {
            "AutheticationToken": self._get_auth_payload(),
            "Data": data if data is not None else (self.config.default_data or {})
        }
        logger.info(f"📡 Requisição para {url} timeout={self.timeout}s")

        for attempt in range(1, self.max_retries + 1):
            try:
                if self.config.method.upper() == "GET":
                    response = self.session.get(url, params=payload, timeout=self.timeout)
                else:
                    response = self.session.post(url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout as e:
                logger.warning(f"⏱️ Timeout na tentativa {attempt}/{self.max_retries}")
                if attempt == self.max_retries:
                    raise
                import time
                time.sleep(self.retry_delay * attempt)
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Erro na tentativa {attempt}: {str(e)}")
                if hasattr(e.response, 'text'):
                    logger.error(f"Resposta: {e.response.text[:200]}")
                if attempt == self.max_retries:
                    raise
                time.sleep(self.retry_delay * attempt)
        raise Exception("Todas as tentativas falharam")

    @abstractmethod
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        pass


class GenericExtractor(BaseExtractor):
    """Implementação genérica do extrator para endpoints simples."""
    def __init__(self, token_manager, config, credentials, spark=None):
        super().__init__(token_manager, config, credentials, spark)

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        try:
            logger.info(f"🔄 GenericExtractor para {self.config.name}")
            response = self._make_request(kwargs.get("data"))
            if response and "Data" in response:
                data = response["Data"]
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    return [data]
            return []
        except Exception as e:
            logger.error(f"❌ Erro: {str(e)}")
            return []