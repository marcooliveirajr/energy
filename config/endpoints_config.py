from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class EndpointConfig:
    name: str
    method: str
    path: str
    base_url: str = "https://mxm-thopenergy.rsmbrasil.com.br"
    default_data: Optional[Dict] = None
    table_name: str = ""
    extraction_type: str = "full"

class MXMEndpoints:
    ENDPOINTS = {
        "consultar_faturas": EndpointConfig(
            name="consultar_faturas",
            method="POST",
            path="/webmanager/api/InterfacedaFatura/ConsultaFaturas",
            table_name="mxm_faturas",
            extraction_type="incremental"
        ),
        # Outros endpoints podem ser adicionados depois, mas vamos focar apenas nesse
    }

    @classmethod
    def get_endpoint(cls, name: str) -> Optional[EndpointConfig]:
        return cls.ENDPOINTS.get(name)