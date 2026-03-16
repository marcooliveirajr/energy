from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, when, to_date, current_timestamp
import logging

logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self, spark):
        self.spark = spark
    
    def transform(self, endpoint_name: str, df: DataFrame) -> DataFrame:
        if df.count() == 0:
            return df
        
        # Transformações comuns
        for col_name in df.columns:
            new_name = col_name.lower() \
                .replace(" ", "_") \
                .replace(".", "_") \
                .replace("-", "_") \
                .replace("ç", "c") \
                .replace("ã", "a") \
                .replace("õ", "o") \
                .replace("á", "a") \
                .replace("é", "e") \
                .replace("í", "i") \
                .replace("ó", "o") \
                .replace("ú", "u")
            
            if new_name != col_name:
                df = df.withColumnRenamed(col_name, new_name)
        
        # Adiciona timestamp de processamento
        df = df.withColumn("_processed_at", current_timestamp())
        
        return df