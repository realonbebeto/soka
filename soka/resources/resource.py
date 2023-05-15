import sqlalchemy
from dagster import Field, String, resource, List
import pandas as pd
import os


class Postgres:
    def __init__(self, host: str, port: str, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._engine = sqlalchemy.create_engine(self.uri)

    @property
    def uri(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def execute_query(self, query: str):
        self._engine.execute(query)

    def ingest_data(self, table: str, data: pd.DataFrame, index_cols: List):
        with self._engine.connect() as conn:
            data.to_sql(name=table, con=conn, if_exists='replace', index=False, index_label=index_cols)
        


@resource(
    config_schema={
        "host": Field(String),
        "port": Field(String),
        "user": Field(String),
        "password": Field(String),
        "database": Field(String),
    },
    description="A resource that can run Postgres instance",
)
def postgres_resource(context) -> Postgres:
    """This resource defines a Postgres client"""
    return Postgres(
        host=context.resource_config["host"],
        port=context.resource_config["port"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        database=context.resource_config["database"],
    )



# @resource()
# def tempdata_dir_resource(context) -> None :
