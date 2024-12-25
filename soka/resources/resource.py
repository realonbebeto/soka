import pandas as pd
import sqlalchemy
from dagster import Field, String, resource
from sqlalchemy import text, inspect
from typing import List


class Postgres:
    def __init__(self, host: str, port: str, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._engine = sqlalchemy.create_engine(self.uri)
        print(self.uri)

    @property
    def uri(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def execute_query(self, query: str):
        return self._engine.execute(query)

    def ingest_data(
        self, schema: str, table: str, data: pd.DataFrame, if_exists: str = "append"
    ):
        with self._engine.connect() as conn:
            data.to_sql(
                name=table, con=conn, schema=schema, if_exists=if_exists, index=False
            )

    def get_min_date(self, schema: str, table: str):
        with self._engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT MIN(date) from {schema}.{table}")
            ).scalar()

        return result
    
    def get_existing_records(self, schema: str, table: str, primary_cols: List):
        with self._engine.connect() as conn:
            insp = inspect(conn)
            if not insp.has_table(table, schema):
                return set()
            
            query = f"SELECT DISTINCT {', '.join(primary_cols)} FROM {schema}.{table}"
            existing_df = pd.read_sql(query, conn)
            existing_df["record_key"] = existing_df[primary_cols].astype(str).agg("_".join, axis=1)
            return set(existing_df["record_key"].to_list())


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
    config = context.resource_config
    return Postgres(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        database=config["database"],
    )


# @resource()
# def tempdata_dir_resource(context) -> None :
