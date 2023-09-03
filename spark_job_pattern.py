from abc import (
  ABC,
  abstractmethod,
)
from typing import List
from pyspark.sql import DataFrame, SparkSession



class SparkJob(ABC):

    def __init__(self) -> None:
        self.spark = SparkSession.builder.appName('spark').getOrCreate()

    @abstractmethod
    def load(self, spark: SparkSession) -> List[DataFrame]:
        """
        Método destinado a carregar os dados brutos em dataframes. Os dataframes devem ser retornados pelo método
        para serem passados ao método de transformação em formato de lista.
        """
        pass

    @abstractmethod
    def transform(self, sources: List[DataFrame]) -> DataFrame:
        """
        Método responsável pelo tratamento dos dados carregados pelo método extract e retornar o dataframe final
        :param sources: lista de Dataframes a serem transformados
        """
        pass

    @abstractmethod
    def save(self, target: DataFrame) -> None:
        """
        Método responsável por carregar o dataframe final no destino
        :param target: Dataframe final a ser carregado
        """
        pass

    def run(self):
        """
        Realiza a execução do pipeline
        """
        spark = SparkSession.builder.appName('spark').getOrCreate()
        self.save(self.transform(self.extract(spark)))
    
    def debug(self):
        """Exibe uma amostra dos dados carregados e transformados para debug e identificação de erros. 
        Nenhum dado é escrito (método save não é chamado).
        """
        spark = SparkSession.builder.appName('spark').getOrCreate()
        print("\n**************** DATA LOADED **********************\n")
        sources = self.extract(spark)
        for df in sources:
            df.show(5)
        print("\n**************** DATA TRANFORMED **********************\n")
        df_final = self.transform(sources)
        df_final.show(5)


