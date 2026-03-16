from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging
import pandas as pd

from config.endpoints_config import MXMEndpoints, EndpointConfig
from config.secrets_config import SecretsManager
from src.auth.token_manager import TokenManager
from src.extractors import (
    BaseExtractor, 
    GenericExtractor,
    ClienteExtractor,
    FornecedorExtractor,
    FaturaExtractor,
    PedidosVendaExtractor,
    SaldoContabilExtractor,
    TitulosExtractor
)
from src.transformers.data_transformer import DataTransformer
from src.writers.delta_writer import DeltaWriter

logger = logging.getLogger(__name__)

class MXMExtractionPipeline:
    """
    Pipeline para endpoints que funcionam normalmente (full load)
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
        
        # Mapeamento de extratores para endpoints normais
        self.extractor_map = {
            "obter_clientes": ClienteExtractor,
            # Os problemáticos NÃO estão aqui - vão para outro pipeline
        }
    
    def create_extractor(self, config: EndpointConfig) -> BaseExtractor:
        """Cria extrator apenas para endpoints normais"""
        extractor_class = self.extractor_map.get(config.name)
        
        if extractor_class:
            logger.info(f"📌 Usando extrator específico para {config.name}")
            return extractor_class(
                token_manager=self.token_manager,
                config=config,
                credentials=self.secrets,
                spark=self.spark
            )
        
        # Para endpoints normais sem extrator específico, usa genérico
        logger.info(f"📌 Usando extrator genérico para {config.name}")
        return GenericExtractor(
            token_manager=self.token_manager,
            config=config,
            credentials=self.secrets,
            spark=self.spark
        )
    
    def extract_endpoint(self, config: EndpointConfig) -> DataFrame:
        """Extrai um endpoint normal"""
        logger.info(f"🚀 Extraindo {config.name}")
        
        try:
            extractor = self.create_extractor(config)
            raw_data = extractor.extract()
            
            if raw_data:
                # Tenta criar DataFrame com tratamento de erros
                try:
                    df = self.spark.createDataFrame(raw_data)
                except:
                    # Fallback para string
                    pdf = pd.DataFrame(raw_data)
                    for col_name in pdf.columns:
                        pdf[col_name] = pdf[col_name].astype(str)
                    df = self.spark.createDataFrame(pdf)
                
                df = df.withColumn("_extracted_at", current_timestamp()) \
                       .withColumn("_endpoint", lit(config.name))
                
                count = df.count()
                logger.info(f"✅ {config.name}: {count} registros")
                return df
            else:
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
        """Executa apenas endpoints normais"""
        logger.info("🔄 Iniciando extração de endpoints normais")
        results = {}
        
        for config in MXMEndpoints.get_normal_endpoints():
            try:
                df = self.extract_endpoint(config)
                if df.count() > 0:
                    results[config.name] = df
            except Exception as e:
                logger.error(f"❌ Falha em {config.name}: {str(e)}")
        
        return results
    
    def save_results(self, results: Dict[str, DataFrame], mode: str = "append"):
        """Salva resultados no Delta Lake"""
        for endpoint_name, df in results.items():
            config = MXMEndpoints.get_endpoint(endpoint_name)
            table_name = config.table_name if config else endpoint_name
            
            transformed_df = self.transformer.transform(endpoint_name, df)
            self.writer.write(df=transformed_df, table_name=table_name, mode=mode)
    
    def get_extraction_log(self) -> List[Dict]:
        return self.extraction_log