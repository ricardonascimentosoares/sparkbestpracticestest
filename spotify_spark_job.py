from pyspark.sql import DataFrame
from spark_job_pattern import SparkJob, SparkJobExecutor


class SpotifySparkJob(SparkJob):

    def extract(self, source_path, reader) -> DataFrame:
        df = reader.format('csv')\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load(source_path)
        df.show()
        return df

    def transform(self, dataset) -> DataFrame:
        df = dataset[dataset.liked == 1] \
            .select('key', 'loudness', 'duration_ms', 'liked') \
            .withColumn('duration_minutes', (dataset.duration_ms / 1000) / 60)
        return df

    def load(self, target_path, dataset):
        dataset.write.mode("overwrite").format('parquet').save(target_path)
        dataset.show()


class SpotifySparkJobExecutor(SparkJobExecutor):
    def execute(self):
        SpotifySparkJob(source_path='data.csv', target_path='results/').run()


SpotifySparkJobExecutor().execute()
