from abc import abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, date
import time
import random
import logging
import requests
from .base_extractor import BaseExtractor
from ..checkpoint.checkpoint_manager import CheckpointManager

logger = logging.getLogger(__name__)

class SmartIncrementalExtractor(BaseExtractor):
    def __init__(self, token_manager, config, credentials, spark=None):
        super().__init__(token_manager, config, credentials, spark)
        self.checkpoint_manager = CheckpointManager(spark) if spark else None
        self.max_retries = 5
        self.base_delay = 5
        self.max_delay = 60
        self.jitter = True
        self.batch_size = 1
        self.timeout = 180  # consistente com base

    def _request_with_backoff(self, data: Dict) -> Optional[Dict]:
        """Faz requisição com backoff exponencial e jitter, capturando exceções."""
        for attempt in range(1, self.max_retries + 1):
            try:
                delay = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
                if self.jitter:
                    delay = delay * (0.5 + random.random())
                if attempt > 1:
                    logger.info(f"Tentativa {attempt}, aguardando {delay:.2f}s...")
                    time.sleep(delay)

                return self._make_request(data)  # reutiliza o método da base

            except requests.exceptions.Timeout as e:
                logger.warning(f"Timeout na tentativa {attempt}")
                if attempt == self.max_retries:
                    logger.error("Máximo de tentativas atingido, desistindo.")
                    return None
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt}: {str(e)}")
                if attempt == self.max_retries:
                    return None
        return None

    @abstractmethod
    def generate_parameters(self, start_date: date, end_date: date) -> List[Dict]:
        pass

    @abstractmethod
    def get_parameter_key(self, params: Dict) -> str:
        pass

    def extract_range(self, start_date: date, end_date: date) -> List[Dict]:
        if not self.checkpoint_manager:
            raise ValueError("CheckpointManager não disponível (spark necessário)")

        all_data = []
        parameters_list = self.generate_parameters(start_date, end_date)

        # Filtra já processados
        pending = []
        for params in parameters_list:
            key = self.get_parameter_key(params)
            last = self.checkpoint_manager.get_last_checkpoint(self.config.name, key)
            if last is None:
                pending.append(params)

        logger.info(f"{len(pending)} períodos pendentes de {len(parameters_list)}")

        for i in range(0, len(pending), self.batch_size):
            batch = pending[i:i+self.batch_size]
            logger.info(f"Lote {i//self.batch_size + 1}/{(len(pending)-1)//self.batch_size + 1}")

            for params in batch:
                response = self._request_with_backoff(params)
                if response and "Data" in response:
                    data = response["Data"]
                    if isinstance(data, list):
                        all_data.extend(data)
                    else:
                        all_data.append(data)
                    key = self.get_parameter_key(params)
                    self.checkpoint_manager.update_checkpoint(self.config.name, key, key)
                    logger.info(f"Período {key} extraído com sucesso.")
                else:
                    logger.warning(f"Falha ao extrair {params}")

            # Pausa entre lotes
            if i + self.batch_size < len(pending):
                time.sleep(5)

        return all_data