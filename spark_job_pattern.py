from abc import (
  ABC,
  abstractmethod,
)

from pyspark.sql import DataFrame, SparkSession, DataFrameReader


class SparkJob(ABC):
    def __init__(self, source_path, target_path):
        self._spark = SparkSession.builder.appName('spark').getOrCreate()
        self._source_path = source_path
        self._target_path = target_path

    @abstractmethod
    def extract(self, source_path: str, reader: DataFrameReader) -> DataFrame:
        """
        Método destinado a carregar os dados brutos em dataframes. Os dataframes devem ser retornados pelo método
        para serem passados ao método de transformação

        :param source_path:
        :param reader: DataFrameReader para carregar os dados (csv, parquet, jdbc, etc...)
        """
        pass

    @abstractmethod
    def transform(self, dataset: DataFrame) -> DataFrame:
        """
        Método responsável por todo o tratamento dos dados carregados pelo método extract e retornar o dataframe final
        """
        pass

    @abstractmethod
    def load(self, target_path: str, dataset: DataFrame):
        """
        Método responsável por carregar o dataframe final no destino
        :param target_path:
        :param dataset: Dataframe final a ser carregado
        """
        pass

    def run(self):
        """
        Realiza a execução do pipeline
        """
        self.load(dataset=self.transform(self.extract(source_path=self._source_path,
                                                      reader=self._spark.read)),
                  target_path=self._target_path)

    def run_spark_sql(self, sqltext: str):
        return self._spark.sql(sqltext)

class SparkJobExecutor (ABC):

    @abstractmethod
    def execute(self):
        """
        Define como as jobs serão executadas
        """
        pass
