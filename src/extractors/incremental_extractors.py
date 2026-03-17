from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, date
import logging
from .smart_incremental_extractor import SmartIncrementalExtractor

logger = logging.getLogger(__name__)

class FaturaExtractor(SmartIncrementalExtractor):
    """
    Extrator para faturas – combina empresas e datas diárias.
    A lista de empresas pode ser fixa ou vir de uma tabela.
    """
    def __init__(self, token_manager, config, credentials, spark=None):
        super().__init__(token_manager, config, credentials, spark)
        self.timeout = 300  # 5 minutos por requisição

    def _get_empresas(self) -> List[str]:
        """
        Retorna a lista de códigos de empresa a serem consultados.
        Pode ser substituído por uma consulta a uma tabela, se existir.
        """
        # Lista fixa de exemplo – ajuste conforme necessário
        return ["0702", "0101", "0202"]  # Adicione as empresas desejadas

    def generate_parameters(self, start_date: date, end_date: date) -> List[Dict]:
        """
        Gera uma lista de parâmetros: para cada empresa e cada dia no intervalo.
        Isso resulta em requisições individuais por dia/empresa.
        """
        params = []
        empresas = self._get_empresas()
        delta = timedelta(days=1)
        current = start_date
        while current <= end_date:
            data_str = current.strftime("%d/%m/%Y")
            for emp in empresas:
                params.append({
                    "CodigoEmpresa": emp,
                    "DataFatura": data_str
                })
            current += delta
        logger.info(f"Total de combinações (empresa x dia): {len(params)}")
        return params

    def get_parameter_key(self, params: Dict) -> str:
        """
        Chave única: empresa + data (YYYYMMDD).
        Exemplo: "0702_20260316"
        """
        data = params["DataFatura"]
        dia, mes, ano = data.split("/")
        return f"{params['CodigoEmpresa']}_{ano}{mes}{dia}"

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Método principal – recebe um intervalo de datas e retorna todos os registros.
        """
        try:
            data_inicio = kwargs.get("data_inicio")
            data_fim = kwargs.get("data_fim")
            if data_inicio and data_fim:
                start = datetime.strptime(data_inicio, "%d/%m/%Y").date()
                end = datetime.strptime(data_fim, "%d/%m/%Y").date()
            else:
                # Padrão: últimos 30 dias
                end = datetime.now().date()
                start = end - timedelta(days=30)

            result = self.extract_range(start, end)
            return result if result is not None else []
        except Exception as e:
            logger.error(f"Erro no extract de FaturaExtractor: {e}")
            return []