#!/usr/bin/env python3

import boto3
import re
from pyhocon import ConfigTree
from typing import Iterator, Union, Dict, Any, List
from databuilder.extractor.base_extractor import Extractor
from carte_cli.model.carte_table_model import TableMetadata, ColumnMetadata, TableType


class GlueExtractor(Extractor):
    def __init__(self, connection_name: str):
        super().__init__()
        self.connection_name = connection_name

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf
        self._glue = boto3.client("glue")
        tbl_filter = conf.get_string("table_name_filter", None)
        self._table_name_re = re.compile(tbl_filter) if tbl_filter else None
        self._extract_iter = None

    def extract(self) -> Union[TableMetadata, None]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        return "carte.extractor.glue"

    def _get_column_type(self, column: Dict) -> str:
        col_type = column["type"]
        if type(col_type) == dict:
            col_sub_type = col_type["type"]
            if col_sub_type == "map":
                return f"map<{col_type['keyType']},{col_type['valueType']}>"
            return col_sub_type
        else:
            return col_type

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        for row in self._get_raw_extract_iter():
            columns = []
            table_name = row["Name"]
            db_name = row["DatabaseName"]
            full_table_name = f"{db_name}.{table_name}"
            if (
                self._table_name_re is not None
                and self._table_name_re.search(full_table_name) is not None
            ):
                continue

            is_view = row.get("TableType") == "VIRTUAL_VIEW"

            if is_view:
                for column in row["StorageDescriptor"]["Columns"]:
                    columns.append(
                        ColumnMetadata(
                            name=column["Name"],
                            column_type=column["Type"],
                            description=None,
                        )
                    )
            else:
                # This solution is more robust for Delta and Parquet tables.
                # Both table types has these fields
                for column in row["Parameters"]["spark.sql.sources.schema"]["fields"]:
                    columns.append(
                        ColumnMetadata(
                            name=column["name"],
                            column_type=self._get_column_type(column),
                            description=None,
                        )
                    )

            table_type = TableType.VIEW if is_view else TableType.TABLE

            yield TableMetadata(
                name=table_name,
                connection=self.connection_name,
                database=db_name,
                description=None,
                location=row["StorageDescriptor"].get("Location", None),
                columns=columns,
                table_type=table_type,
            )

    def _get_raw_extract_iter(self) -> Iterator[Dict[str, Any]]:
        tables = self._search_tables()
        return iter(tables)

    def _search_tables(self) -> List[Dict[str, Any]]:
        tables = []
        kwargs = {}
        data = self._glue.search_tables()
        tables += data["TableList"]

        while "NextToken" in data:
            token = data["NextToken"]
            kwargs["NextToken"] = token
            data = self._glue.search_tables(**kwargs)
            tables += data["TableList"]

        return tables
