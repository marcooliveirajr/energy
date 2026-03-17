from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime
from typing import Dict, List, Optional
import logging
import pandas as pd

from config.endpoints_config import MXMEndpoints, EndpointConfig
from config.secrets_config import SecretsManager
from src.auth.token_manager import TokenManager
from src.extractors.base_extractor import BaseExtractor, GenericExtractor
from src.extractors.cliente_extractor import ClienteExtractor
from src.transformers.data_transformer import DataTransformer
from src.writers.delta_writer import DeltaWriter

logger = logging.getLogger(__name__)

class MXMExtractionPipeline:
    """
    Pipeline para endpoints que funcionam em modo full (não incrementais).
    """
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
        self.extraction_log = []

        # Mapeamento de extratores específicos (apenas para endpoints que já funcionaram)
        self.extractor_map = {
            "obter_clientes": ClienteExtractor,
            # Outros podem ser adicionados conforme necessidade, mas por enquanto apenas ClienteExtractor
        }

    def create_extractor(self, config: EndpointConfig) -> BaseExtractor:
        """Retorna o extrator adequado para o endpoint."""
        extractor_class = self.extractor_map.get(config.name)
        if extractor_class:
            logger.info(f"📌 Usando extrator específico para {config.name}")
            return extractor_class(
                token_manager=self.token_manager,
                config=config,
                credentials=self.secrets,
                spark=self.spark
            )
        else:
            # Para endpoints sem extrator específico, usa o GenericExtractor
            logger.info(f"📌 Usando extrator genérico para {config.name}")
            return GenericExtractor(
                token_manager=self.token_manager,
                config=config,
                credentials=self.secrets,
                spark=self.spark
            )

    def extract_endpoint(self, config: EndpointConfig) -> DataFrame:
        logger.info(f"🚀 Extraindo {config.name}")
        try:
            extractor = self.create_extractor(config)
            raw_data = extractor.extract()
            if raw_data:
                # Tenta criar DataFrame; se falhar, converte tudo para string
                try:
                    df = self.spark.createDataFrame(raw_data)
                except:
                    pdf = pd.DataFrame(raw_data)
                    for col_name in pdf.columns:
                        pdf[col_name] = pdf[col_name].astype(str)
                    df = self.spark.createDataFrame(pdf)
                df = df.withColumn("_extracted_at", current_timestamp()) \
                       .withColumn("_endpoint", lit(config.name))
                logger.info(f"✅ {config.name}: {df.count()} registros")
                return df
            else:
                logger.warning(f"⚠️ Nenhum dado para {config.name}")
                return self.spark.createDataFrame([], StructType())
        except Exception as e:
            logger.error(f"❌ Erro em {config.name}: {str(e)}")
            self.extraction_log.append({
                "endpoint": config.name,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
            return self.spark.createDataFrame([], StructType())

    def run_normal_extraction(self) -> Dict[str, DataFrame]:
        """Executa extração de todos os endpoints normais (extraction_type='full')."""
        logger.info("🔄 Iniciando extração de endpoints normais")
        results = {}
        for config in MXMEndpoints.get_all_endpoints():
            if config.extraction_type == "full":
                df = self.extract_endpoint(config)
                if df.count() > 0:
                    results[config.name] = df
        return results

    def save_results(self, results: Dict[str, DataFrame], mode: str = "append"):
        for endpoint_name, df in results.items():
            config = MXMEndpoints.get_endpoint(endpoint_name)
            table_name = config.table_name if config else endpoint_name
            transformed = self.transformer.transform(endpoint_name, df)
            self.writer.write(transformed, table_name=table_name, mode=mode)

    def get_extraction_log(self) -> List[Dict]:
        return self.extraction_log