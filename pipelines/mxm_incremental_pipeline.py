from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
import logging
import pandas as pd
import time

from config.endpoints_config import MXMEndpoints
from config.secrets_config import SecretsManager
from src.auth.token_manager import TokenManager
from src.extractors.incremental_extractors import (
    FaturaExtractor,
    PedidosVendaExtractor,
    TitulosExtractor,
    SaldoContabilExtractor
)
from src.transformers.data_transformer import DataTransformer
from src.writers.delta_writer import DeltaWriter
from src.checkpoint.checkpoint_manager import CheckpointManager

logger = logging.getLogger(__name__)

class MXMIncrementalPipeline:
    def __init__(self, spark: SparkSession):
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
        extractor_class = self.extractor_map.get(endpoint_name)
        if extractor_class:
            logger.info(f"📌 Usando extrator para {endpoint_name}")
            return extractor_class(
                token_manager=self.token_manager,
                config=config,
                credentials=self.secrets,
                spark=self.spark
            )
        else:
            logger.error(f"❌ Sem extrator para {endpoint_name}")
            return None

    def run_incremental(self, days_back: int = 30) -> Dict[str, DataFrame]:
        logger.info(f"🔄 Iniciando extração incremental dos últimos {days_back} dias")
        results = {}
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)

        endpoints = [
            ("consultar_faturas", "faturas"),
            ("pedidos_vendas", "pedidos"),
            ("titulos_receber", "títulos a receber"),
            ("titulos_pagar", "títulos a pagar"),
            ("consulta_saldo_contabil", "saldo contábil")
        ]

        for endpoint_name, desc in endpoints:
            logger.info(f"\n{'='*50}\nProcessando {desc}...\n{'='*50}")
            extractor = self.create_extractor(endpoint_name)
            if not extractor:
                continue

            try:
                # Cada extração tem seu próprio tratamento de exceção para não travar as demais
                if endpoint_name in ["titulos_receber", "titulos_pagar"]:
                    tipo = "receber" if "receber" in endpoint_name else "pagar"
                    raw_data = extractor.extract(
                        tipo=tipo,
                        data_inicio=start_date.strftime("%d/%m/%Y"),
                        data_fim=end_date.strftime("%d/%m/%Y")
                    )
                else:
                    raw_data = extractor.extract(
                        data_inicio=start_date.strftime("%d/%m/%Y"),
                        data_fim=end_date.strftime("%d/%m/%Y")
                    )

                if raw_data:
                    pdf = pd.DataFrame(raw_data)
                    # Converte todas as colunas para string para evitar problemas de schema
                    for col_name in pdf.columns:
                        pdf[col_name] = pdf[col_name].astype(str)
                    df = self.spark.createDataFrame(pdf)
                    df = df.withColumn("_extracted_at", current_timestamp()) \
                           .withColumn("_endpoint", lit(endpoint_name))
                    results[endpoint_name] = df
                    logger.info(f"✅ {desc}: {df.count()} registros")
                else:
                    logger.info(f"ℹ️ {desc}: nenhum dado novo")
            except Exception as e:
                logger.error(f"❌ Erro ao processar {desc}: {str(e)}")
                self.extraction_log.append({
                    "endpoint": endpoint_name,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
            # Pausa entre endpoints para não sobrecarregar
            time.sleep(5)

        return results

    def save_results(self, results: Dict[str, DataFrame], mode: str = "append"):
        for endpoint_name, df in results.items():
            config = MXMEndpoints.get_endpoint(endpoint_name)
            table_name = config.table_name if config else endpoint_name
            transformed_df = self.transformer.transform(endpoint_name, df)
            self.writer.write(df=transformed_df, table_name=table_name, mode=mode)
            logger.info(f"💾 {table_name}: {transformed_df.count()} registros salvos")

    def get_extraction_log(self) -> List[Dict]:
        return self.extraction_log