from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

class DeltaWriter:
    def __init__(self, spark):
        self.spark = spark
    
    def write(self, df: DataFrame, table_name: str, database: str = "bronze", 
              mode: str = "append", partition_cols=None):
        
        full_table_name = f"{database}.{table_name}"
        
        logger.info(f"Escrevendo {df.count()} registros em {full_table_name}")
        
        writer = df.write.format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        try:
            writer.saveAsTable(full_table_name)
            logger.info(f"Escrita concluída")
        except Exception as e:
            logger.error(f"Erro ao escrever: {str(e)}")
            # Tenta criar a tabela
            df.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(full_table_name)