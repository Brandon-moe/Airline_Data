from abc import ABC, abstractmethod
import pandas as pd
from google.cloud import bigquery


# Abstract class that can be used as an interface
class DataLoadingRepository(ABC):

    @abstractmethod
    def load_data(self, df: pd.DataFrame, table_id: str,
                  write_disposition: str) -> None:
        pass

    @abstractmethod
    def load_job_config(self, write_disposition: str):
        pass


    @abstractmethod
    def extract_data(self,query: str):
        pass

class BigQueryDataLoadingRepository(DataLoadingRepository):

    __schema = [
        bigquery.SchemaField("hex", "STRING"),
        bigquery.SchemaField("dep_country", "STRING"),
        bigquery.SchemaField("dep_lat", "STRING"),
        bigquery.SchemaField("dep_long", "STRING"),
        bigquery.SchemaField("dep_airport", "STRING"),
        bigquery.SchemaField("arr_country", "STRING"),
        bigquery.SchemaField("arr_lat", "STRING"),
        bigquery.SchemaField("arr_long", "STRING"),
        bigquery.SchemaField("arr_airport", "STRING"),
        bigquery.SchemaField("Pax", "INTEGER"),
        bigquery.SchemaField("timestamp", "INTEGER")
    ]

    def __init__(self, client: bigquery.Client, dataset_id: str):
        self.client = client
        self.dataset_id = dataset_id

    def load_data(self, df: pd.DataFrame, table_id: str,
                  write_disposition: str) -> None:
        table_ref = self.client.dataset(self.dataset_id).table(table_id)
        job_config = self.load_job_config(write_disposition)
        self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    def load_job_config(self, write_disposition: str) -> bigquery.LoadJobConfig:
        return bigquery.LoadJobConfig(
            schema=self.__schema,
            write_disposition=write_disposition
        )

    def extract_data(self,query: str):
        return self.client.query(query).result()
