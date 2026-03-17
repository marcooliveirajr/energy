from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, date
import logging
from .smart_incremental_extractor import SmartIncrementalExtractor

logger = logging.getLogger(__name__)

class EmpresaSource:
    """
    Classe auxiliar para obter lista de empresas de uma tabela ou fallback.
    """
    def __init__(self, spark=None):
        self.spark = spark

    def get_empresas(self) -> List[str]:
        """
        Retorna lista de códigos de empresa.
        Se spark disponível, tenta buscar da tabela bronze.mxm_empresas.
        Caso contrário, retorna lista fixa.
        """
        if self.spark:
            try:
                df = self.spark.sql("SELECT DISTINCT codigo FROM bronze.mxm_empresas WHERE codigo IS NOT NULL")
                empresas = [row.codigo for row in df.collect()]
                if empresas:
                    logger.info(f"Empresas carregadas da tabela: {empresas}")
                    return empresas
                else:
                    logger.warning("Tabela de empresas vazia, usando fallback.")
            except Exception as e:
                logger.warning(f"Erro ao consultar tabela de empresas: {e}. Usando fallback.")
        # Fallback
        fallback = ["0702", "0101", "0202"]  # ajuste conforme necessário
        logger.info(f"Usando lista fixa de empresas: {fallback}")
        return fallback


class FaturaExtractor(SmartIncrementalExtractor):
    """
    Extrator para faturas – combina empresas (da tabela) e datas diárias.
    """
    def __init__(self, token_manager, config, credentials, spark=None):
        super().__init__(token_manager, config, credentials, spark)
        self.timeout = 300
        self.empresa_source = EmpresaSource(spark)

    def _get_empresas(self) -> List[str]:
        return self.empresa_source.get_empresas()

    def generate_parameters(self, start_date: date, end_date: date) -> List[Dict]:
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
        logger.info(f"Faturas: {len(params)} combinações (empresa x dia)")
        return params

    def get_parameter_key(self, params: Dict) -> str:
        data = params["DataFatura"]
        dia, mes, ano = data.split("/")
        return f"{params['CodigoEmpresa']}_{ano}{mes}{dia}"

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        try:
            data_inicio = kwargs.get("data_inicio")
            data_fim = kwargs.get("data_fim")
            if data_inicio and data_fim:
                start = datetime.strptime(data_inicio, "%d/%m/%Y").date()
                end = datetime.strptime(data_fim, "%d/%m/%Y").date()
            else:
                end = datetime.now().date()
                start = end - timedelta(days=30)
            result = self.extract_range(start, end)
            return result if result is not None else []
        except Exception as e:
            logger.error(f"Erro FaturaExtractor.extract: {e}")
            return []


class FornecedorExtractor(SmartIncrementalExtractor):
    """
    Extrator para fornecedores – pode ser incremental mas geralmente é full.
    Vamos manter simples, sem quebra por período.
    """
    def __init__(self, token_manager, config, credentials, spark=None):
        super().__init__(token_manager, config, credentials, spark)
        self.timeout = 180

    def generate_parameters(self, start_date: date, end_date: date) -> List[Dict]:
        # Para fornecedores, não há período, então retornamos um único parâmetro vazio
        return [{}]

    def get_parameter_key(self, params: Dict) -> str:
        return "todos"

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        try:
            # Ignora datas, faz uma única requisição
            result = self.extract_range(datetime.now().date(), datetime.now().date())
            return result if result is not None else []
        except Exception as e:
            logger.error(f"Erro FornecedorExtractor.extract: {e}")
            return []


class PedidosVendaExtractor(SmartIncrementalExtractor):
    """
    Extrator para pedidos de venda – por período (diário).
    """
    def __init__(self, token_manager, config, credentials, spark=None):
        super().__init__(token_manager, config, credentials, spark)
        self.timeout = 240

    def generate_parameters(self, start_date: date, end_date: date) -> List[Dict]:
        params = []
        delta = timedelta(days=1)
        current = start_date
        while current <= end_date:
            data_str = current.strftime("%d/%m/%Y")
            params.append({
                "DataEmissaoInicio": data_str,
                "DataEmissaoFim": data_str
            })
            current += delta
        return params

    def get_parameter_key(self, params: Dict) -> str:
        data = params["DataEmissaoInicio"]
        dia, mes, ano = data.split("/")
        return f"{ano}{mes}{dia}"

    def _make_request(self, data: Optional[Dict] = None) -> Dict[str, Any]:
        # Força POST com JSON (alguns endpoints podem exigir)
        url = f"{self.config.base_url}{self.config.path}"
        payload = {
            "AutheticationToken": self._get_auth_payload(),
            "Data": data or {}
        }
        headers = {"Content-Type": "application/json"}
        logger.info(f"📡 POST {url}")
        resp = self.session.post(url, json=payload, headers=headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        try:
            data_inicio = kwargs.get("data_inicio")
            data_fim = kwargs.get("data_fim")
            if data_inicio and data_fim:
                start = datetime.strptime(data_inicio, "%d/%m/%Y").date()
                end = datetime.strptime(data_fim, "%d/%m/%Y").date()
            else:
                end = datetime.now().date()
                start = end - timedelta(days=30)
            result = self.extract_range(start, end)
            return result if result is not None else []
        except Exception as e:
            logger.error(f"Erro PedidosVendaExtractor.extract: {e}")
            return []


class TitulosExtractor(SmartIncrementalExtractor):
    """
    Extrator para títulos a pagar/receber – por dia.
    """
    def __init__(self, token_manager, config, credentials, spark=None):
        super().__init__(token_manager, config, credentials, spark)
        self.timeout = 120

    def _get_correct_url(self, tipo: str) -> str:
        base = "https://mxm-thopenergy.rsmbrasil.com.br/webmanager/api"
        if tipo == "receber":
            return f"{base}/InterfacedoContasPagarReceber/ConsultaTituloReceber"
        else:
            return f"{base}/InterfacedoContasPagarReceber/ConsultaTituloPagar"

    def generate_parameters(self, start_date: date, end_date: date) -> List[Dict]:
        params = []
        delta = timedelta(days=1)
        current = start_date
        while current <= end_date:
            data_str = current.strftime("%d/%m/%Y")
            params.append({
                "DataVencimentoInicio": data_str,
                "DataVencimentoFim": data_str
            })
            current += delta
        return params

    def get_parameter_key(self, params: Dict) -> str:
        data = params["DataVencimentoInicio"]
        dia, mes, ano = data.split("/")
        return f"{ano}{mes}{dia}"

    def _make_request(self, data: Optional[Dict] = None) -> Dict[str, Any]:
        tipo = getattr(self, '_current_tipo', 'receber')
        original_path = self.config.path
        self.config.path = self._get_correct_url(tipo).replace(self.config.base_url, "")
        try:
            return super()._make_request(data)
        finally:
            self.config.path = original_path

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        try:
            self._current_tipo = kwargs.get("tipo", "receber")
            data_inicio = kwargs.get("data_inicio")
            data_fim = kwargs.get("data_fim")
            if data_inicio and data_fim:
                start = datetime.strptime(data_inicio, "%d/%m/%Y").date()
                end = datetime.strptime(data_fim, "%d/%m/%Y").date()
            else:
                end = datetime.now().date()
                start = end - timedelta(days=30)
            result = self.extract_range(start, end)
            return result if result is not None else []
        except Exception as e:
            logger.error(f"Erro TitulosExtractor.extract: {e}")
            return []


class SaldoContabilExtractor(SmartIncrementalExtractor):
    """
    Extrator para saldo contábil – por mês (AnoMes).
    """
    def __init__(self, token_manager, config, credentials, spark=None):
        super().__init__(token_manager, config, credentials, spark)
        self.timeout = 120
        self.empresa_source = EmpresaSource(spark)

    def _get_empresas(self) -> List[str]:
        return self.empresa_source.get_empresas()

    def generate_parameters(self, start_date: date, end_date: date) -> List[Dict]:
        params = []
        empresas = self._get_empresas()
        current = start_date.replace(day=1)
        while current <= end_date:
            ano_mes = current.strftime("%Y%m")
            for emp in empresas:
                params.append({
                    "CodigoEmpresa": emp,
                    "AnoMes": ano_mes,
                    "CodigoContaContabil": ""
                })
            # próximo mês
            if current.month == 12:
                current = current.replace(year=current.year+1, month=1)
            else:
                current = current.replace(month=current.month+1)
        logger.info(f"Saldo contábil: {len(params)} combinações (empresa x mês)")
        return params

    def get_parameter_key(self, params: Dict) -> str:
        return f"{params['CodigoEmpresa']}_{params['AnoMes']}"

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        try:
            mes_ano = kwargs.get("mes_ano")
            if mes_ano:
                ano = int(mes_ano[:4])
                mes = int(mes_ano[4:])
                start = date(ano, mes, 1)
                end = start
            else:
                end = datetime.now().date()
                start = end.replace(day=1)
            result = self.extract_range(start, end)
            return result if result is not None else []
        except Exception as e:
            logger.error(f"Erro SaldoContabilExtractor.extract: {e}")
            return []