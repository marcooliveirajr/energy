from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, lit
from datetime import datetime, date
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class CheckpointManager:
    def __init__(self, spark: SparkSession, database: str = "bronze"):
        self.spark = spark
        self.database = database
        self.table_name = f"{database}.mxm_extraction_checkpoints"
        self._ensure_table()

    def _ensure_table(self):
        try:
            self.spark.sql(f"SELECT 1 FROM {self.table_name} LIMIT 1").collect()
        except:
            logger.info("Criando tabela de checkpoints...")
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    endpoint STRING,
                    parameter_key STRING,
                    last_value STRING,
                    last_success TIMESTAMP,
                    created_at TIMESTAMP
                ) USING DELTA
            """)

    def get_last_checkpoint(self, endpoint: str, parameter_key: str) -> Optional[str]:
        try:
            df = self.spark.sql(f"""
                SELECT last_value 
                FROM {self.table_name}
                WHERE endpoint = '{endpoint}' 
                  AND parameter_key = '{parameter_key}'
                ORDER BY last_success DESC
                LIMIT 1
            """)
            rows = df.collect()
            if rows:
                return rows[0]["last_value"]
        except:
            pass
        return None

    def update_checkpoint(self, endpoint: str, parameter_key: str, last_value: str):
        now = datetime.now()
        df = self.spark.createDataFrame([(
            endpoint, parameter_key, last_value, now, now
        )], schema="endpoint STRING, parameter_key STRING, last_value STRING, last_success TIMESTAMP, created_at TIMESTAMP")
        df.write.format("delta").mode("append").saveAsTable(self.table_name)
        logger.info(f"Checkpoint atualizado: {endpoint} / {parameter_key} = {last_value}")