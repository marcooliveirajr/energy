from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime
from typing import Dict, List
import logging
import pandas as pd

from config.endpoints_config import MXMEndpoints
from config.secrets_config import SecretsManager
from src.auth.token_manager import TokenManager
from src.extractors.base_extractor import BaseExtractor, GenericExtractor
from src.extractors.cliente_extractor import ClienteExtractor
from src.transformers.data_transformer import DataTransformer
from src.writers.delta_writer import DeltaWriter

logger = logging.getLogger(__name__)

class MXMExtractionPipeline:
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
        self.extraction_log = []

        # Mapeamento de extratores específicos para endpoints full (se houver)
        self.extractor_map = {
            "obter_clientes": ClienteExtractor,
            # outros podem ser adicionados
        }

    def create_extractor(self, config):
        classe = self.extractor_map.get(config.name)
        if classe:
            return classe(
                token_manager=self.token_manager,
                config=config,
                credentials=self.secrets,
                spark=self.spark
            )
        else:
            # Usa extrator genérico para os demais
            return GenericExtractor(
                token_manager=self.token_manager,
                config=config,
                credentials=self.secrets,
                spark=self.spark
            )

    def run_full_extraction(self) -> Dict[str, DataFrame]:
        results = {}
        for config in MXMEndpoints.get_full_endpoints():
            logger.info(f"Extraindo {config.name}")
            extractor = self.create_extractor(config)
            try:
                raw = extractor.extract()
                if raw:
                    pdf = pd.DataFrame(raw)
                    for c in pdf.columns:
                        pdf[c] = pdf[c].astype(str)
                    df = self.spark.createDataFrame(pdf)
                    df = df.withColumn("_extracted_at", current_timestamp()) \
                           .withColumn("_endpoint", lit(config.name))
                    results[config.name] = df
                    logger.info(f"✅ {config.name}: {df.count()} registros")
                else:
                    logger.info(f"ℹ️ {config.name}: sem dados")
            except Exception as e:
                logger.error(f"❌ Erro em {config.name}: {e}")
                self.extraction_log.append({
                    "endpoint": config.name,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
        return results

    def save_results(self, results: Dict[str, DataFrame], mode: str = "append"):
        for endpoint_name, df in results.items():
            config = MXMEndpoints.get_endpoint(endpoint_name)
            table_name = config.table_name if config else endpoint_name
            transformed = self.transformer.transform(endpoint_name, df)
            self.writer.write(transformed, table_name=table_name, mode=mode)

    def get_extraction_log(self) -> List[Dict]:
        return self.extraction_log