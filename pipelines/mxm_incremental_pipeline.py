from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import pandas as pd
import time

from config.endpoints_config import MXMEndpoints
from config.secrets_config import SecretsManager
from src.auth.token_manager import TokenManager
from src.extractors.incremental_extractors import (
    FaturaExtractor,
    FornecedorExtractor,
    PedidosVendaExtractor,
    TitulosExtractor,
    SaldoContabilExtractor
)
from src.transformers.data_transformer import DataTransformer
from src.writers.delta_writer import DeltaWriter
from src.checkpoint.checkpoint_manager import CheckpointManager

logger = logging.getLogger(__name__)

class MXMIncrementalPipeline:
    def __init__(self, spark):
        self.spark = spark
        self.secrets = SecretsManager.get_credentials()
        self.token_manager = TokenManager(
            client_id=self.secrets["client_id"],
            client_secret=self.secrets["client_secret"],
            token_url=self.secrets["token_url"]
        )
        self.transformer = DataTransformer(spark)
        self.writer = DeltaWriter(spark)
        self.checkpoint_manager = CheckpointManager(spark)
        self.extraction_log = []

        self.extractor_map = {
            "consultar_faturas": FaturaExtractor,
            "consultar_fornecedor": FornecedorExtractor,
            "pedidos_vendas": PedidosVendaExtractor,
            "titulos_receber": TitulosExtractor,
            "titulos_pagar": TitulosExtractor,
            "consulta_saldo_contabil": SaldoContabilExtractor,
        }

    def create_extractor(self, endpoint_name: str):
        config = MXMEndpoints.get_endpoint(endpoint_name)
        if not config:
            logger.error(f"Endpoint {endpoint_name} não encontrado")
            return None
        classe = self.extractor_map.get(endpoint_name)
        if not classe:
            logger.error(f"Sem extrator para {endpoint_name}")
            return None
        return classe(
            token_manager=self.token_manager,
            config=config,
            credentials=self.secrets,
            spark=self.spark
        )

    def run_incremental(self, days_back: int = 30) -> Dict[str, DataFrame]:
        """
        Executa extração incremental para todos os endpoints configurados.
        Retorna dicionário endpoint -> DataFrame.
        """
        logger.info(f"🔄 Extração incremental dos últimos {days_back} dias")
        results = {}
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)

        for endpoint_name in self.extractor_map.keys():
            logger.info(f"\n--- Processando {endpoint_name} ---")
            extractor = self.create_extractor(endpoint_name)
            if not extractor:
                continue

            try:
                if endpoint_name in ["titulos_receber", "titulos_pagar"]:
                    tipo = "receber" if "receber" in endpoint_name else "pagar"
                    raw = extractor.extract(
                        tipo=tipo,
                        data_inicio=start_date.strftime("%d/%m/%Y"),
                        data_fim=end_date.strftime("%d/%m/%Y")
                    )
                else:
                    raw = extractor.extract(
                        data_inicio=start_date.strftime("%d/%m/%Y"),
                        data_fim=end_date.strftime("%d/%m/%Y")
                    )

                if raw:
                    pdf = pd.DataFrame(raw)
                    # Converte todas as colunas para string para evitar problemas de schema
                    for c in pdf.columns:
                        pdf[c] = pdf[c].astype(str)
                    df = self.spark.createDataFrame(pdf)
                    df = df.withColumn("_extracted_at", current_timestamp()) \
                           .withColumn("_endpoint", lit(endpoint_name))
                    results[endpoint_name] = df
                    logger.info(f"✅ {endpoint_name}: {df.count()} registros")
                else:
                    logger.info(f"ℹ️ {endpoint_name}: nenhum dado")
            except Exception as e:
                logger.error(f"❌ Erro em {endpoint_name}: {e}")
                self.extraction_log.append({
                    "endpoint": endpoint_name,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
            time.sleep(5)

        return results

    def save_results(self, results: Dict[str, DataFrame], mode: str = "append"):
        for endpoint_name, df in results.items():
            config = MXMEndpoints.get_endpoint(endpoint_name)
            table_name = config.table_name if config else endpoint_name
            transformed = self.transformer.transform(endpoint_name, df)
            self.writer.write(transformed, table_name=table_name, mode=mode)

    def get_extraction_log(self) -> List[Dict]:
        return self.extraction_log