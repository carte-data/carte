#!/usr/bin/env python3

import boto3
from pyhocon import ConfigTree
from typing import Iterator, Union, Dict, Any
from databuilder.extractor.base_extractor import Extractor
from extractor.model.carte_table_model import TableMetadata, ColumnMetadata, TableType


class GlueExtractor(Extractor):
    def init(self, conf: ConfigTree) -> None:
        self.conf = conf
        self._cluster = conf.get_string
        self._glue = boto3.client("glue")
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

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        for row in self._get_raw_extract_iter():
            columns = []

            for column in row["StorageDescriptor"]["Columns"] + row.get(
                "PartitionKeys", []
            ):
                columns.append(
                    ColumnMetadata(
                        name=column["Name"],
                        column_type=column["Type"],
                        description=None,
                    )
                )

            table_type = (
                TableType.VIEW
                if (row.get("TableType") == "VIRTUAL_VIEW")
                else TableType.TABLE
            )

            yield TableMetadata(
                name=row["Name"],
                connection="glue",
                database=row["DatabaseName"],
                description=None,
                location=row["StorageDescriptor"].get("Location", ""),
                columns=columns,
                table_type=table_type,
            )

    def _get_raw_extract_iter(self) -> Iterator[Dict[str, Any]]:
        tables = []
        kwargs = {}
        data = self._glue.search_tables()
        tables += data["TableList"]

        while "NextToken" in data:
            token = data["NextToken"]
            kwargs["NextToken"] = token
            data = self._glue.search_tables(**kwargs)
            tables += data["TableList"]

        return iter(tables)
