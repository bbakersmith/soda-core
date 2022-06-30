from __future__ import annotations

import logging
from datetime import datetime

import ibm_db_dbi
from soda.common.logs import Logs
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType

logger = logging.getLogger(__name__)


class Db2DataSource(DataSource):

    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        DataType.TEXT: "VARCHAR(255)",
        DataType.INTEGER: "INT",
        DataType.DECIMAL: "FLOAT",
        DataType.DATE: "DATE",
        DataType.TIME: "TIME",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.TIMESTAMP_TZ: "TIMESTAMP",
        DataType.BOOLEAN: "BOOLEAN",
    }

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)
        self.host = data_source_properties.get("host")
        self.port = data_source_properties.get("port")
        self.password = data_source_properties.get("password")
        self.username = data_source_properties.get("username")
        self.database = data_source_properties.get("database")
        self.schema = data_source_properties.get("schema")
        self.update_schema(self.schema)

    def connect(self):
        conn_str = (
            f"DATABASE={self.database};HOSTNAME={self.host};PORT={self.port};UID={self.username};PWD={self.password}"
        )
        self.connection = ibm_db_dbi.connect(conn_str)
        return self.connection

    def validate_configuration(self, logs: Logs) -> None:
        pass

    def sql_information_schema_tables(self) -> str:
        return "SYSCAT.TABLES"

    def sql_find_table_names(
        self,
        filter: str | None = None,
        include_tables: list[str] = [],
        exclude_tables: list[str] = [],
        table_column_name: str = "table_name",
        schema_column_name: str = "table_schema",
    ) -> str:
        return super().sql_find_table_names(
            filter, include_tables, exclude_tables, table_column_name="TABNAME", schema_column_name="TABSCHEMA"
        )

    def literal_datetime(self, datetime: datetime):
        formatted = datetime.strftime("%Y-%m-%d-%H.%M.%S")
        return f"TIMESTAMP '{formatted}'"
