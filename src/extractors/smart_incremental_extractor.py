from abc import abstractmethod
from typing import List, Dict, Any, Optional
from datetime import date, timedelta
import time
import random
import logging
from .base_extractor import BaseExtractor
from ..checkpoint.checkpoint_manager import CheckpointManager

logger = logging.getLogger(__name__)

class SmartIncrementalExtractor(BaseExtractor):
    """
    Extrator inteligente com checkpoint e backoff.
    """
    def __init__(self, token_manager, config, credentials, spark=None):
        super().__init__(token_manager, config, credentials, spark)
        self.checkpoint_manager = CheckpointManager(spark) if spark else None
        self.base_delay = 5
        self.max_delay = 60
        self.jitter = True
        self.batch_size = 10  # número de combinações por lote (para paralelismo, se desejado)

    def _request_with_backoff(self, data: Dict) -> Optional[Dict]:
        for attempt in range(1, self.max_retries + 1):
            try:
                delay = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
                if self.jitter:
                    delay *= (0.5 + random.random())
                if attempt > 1:
                    logger.info(f"Tentativa {attempt}, aguardando {delay:.1f}s...")
                    time.sleep(delay)

                return self._make_request(data)

            except Exception as e:
                logger.warning(f"Falha tentativa {attempt}: {e}")
                if attempt == self.max_retries:
                    logger.error("Máximo de tentativas – desistindo.")
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
        param_list = self.generate_parameters(start_date, end_date)

        # Filtra apenas os não processados
        pendentes = []
        for params in param_list:
            key = self.get_parameter_key(params)
            if self.checkpoint_manager.get_last_checkpoint(self.config.name, key) is None:
                pendentes.append(params)

        logger.info(f"{len(pendentes)} períodos pendentes de {len(param_list)}")

        # Processa em lotes (aqui batch_size pode ser 1 para evitar muitas requisições simultâneas)
        for i in range(0, len(pendentes), self.batch_size):
            batch = pendentes[i:i+self.batch_size]
            logger.info(f"Lote {i//self.batch_size + 1}/{(len(pendentes)-1)//self.batch_size + 1}")

            for params in batch:
                resp = self._request_with_backoff(params)
                if resp and "Data" in resp:
                    data = resp["Data"]
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
            if i + self.batch_size < len(pendentes):
                time.sleep(5)

        return all_data