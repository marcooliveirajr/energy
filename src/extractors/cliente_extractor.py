from typing import List, Dict, Any
import logging
from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

class ClienteExtractor(BaseExtractor):
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        try:
            logger.info("👥 Extraindo clientes...")
            response = self._make_request()
            if response and "Data" in response:
                data = response["Data"]
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    if "Clientes" in data:
                        clientes = data["Clientes"]
                        return clientes if isinstance(clientes, list) else [clientes]
                    else:
                        return [data]
            return []
        except Exception as e:
            logger.error(f"Erro ao extrair clientes: {str(e)}")
            return []