from typing import List
from pyspark.sql import DataFrame
from spark_job_pattern import SparkJob
import pyspark.sql.functions as F


class SpotifySparkJob(SparkJob):

    def load(self, spark) -> List[DataFrame]:
        df = (spark
              .read
              .format('csv')
              .option("header", "true")
              .option("inferSchema", "true")
              .load("data.csv")
              )
        return [df]

    def transform(self, sources: List[DataFrame]) -> DataFrame:
        df = sources[0]
        df = (df
              .select('key', 'loudness', 'duration_ms', 'liked')
              .withColumn('duration_minutes', (F.col('duration_ms') / 1000) / 60)
              )
        return df

    def save(self, target: DataFrame):
        (target
         .write
         .mode("overwrite")
         .format('parquet')
         .save("results/")
         )


SpotifySparkJob().debug()
