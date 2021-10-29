from typing import List, Union, Iterator, Any, Iterable, Dict
from carte_cli.model.carte_table_model import (
    TableMetadata,
    ColumnMetadata,
    TableType,
    TableTag,
)
from carte_cli.utils.file_io import read_json
from databuilder.extractor.base_extractor import Extractor
from pyhocon import ConfigTree
import boto3
import json
import copy
import re


class JSONSchemaExtractor(Extractor):

    SCHEMA_PATH_KEY = "schema_path"
    S3_PROTOCOL = "s3://"
    DESCRIPTION_KEY = "description"
    DATABASE_KEY = "database"

    def __init__(
        self,
        connection_name: str,
        database: str,
    ):
        super().__init__()
        self.connection_name = connection_name
        self.database = database

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf
        self.s3 = boto3.resource("s3")
        self.schema_path = conf.get_string(self.SCHEMA_PATH_KEY)
        self.pivot_column = conf.get_string("pivot_column", None)
        self.object_expand = conf.get_list("object_expand", None)
        self.extract_descriptions = conf.get_bool("extract_descriptions", False)
        pin_column = conf.get_string("pin_column", None)
        if pin_column is not None:
            self.pin_column = re.compile(pin_column)
        else:
            self.pin_column = None
        filter_columns = conf.get_string("filter_columns", None)
        if filter_columns is not None:
            self.filter_columns = re.compile(filter_columns)
        else:
            self.filter_columns = None
        tags_key = conf.get_string("tags_key", None)
        if tags_key is not None:
            self.tags_key = tags_key.split(".")
        else:
            self.tags_key = None

        self._extract_iter = iter(self._get_extract_iter())

    @classmethod
    def required_config_keys(cls):
        return [cls.SCHEMA_PATH_KEY, cls.DATABASE_KEY]

    def extract(self) -> Any:
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        return f"carte.extractor.json_schema.{self.connection_name}.{self.database}"

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        schema = self._get_schema()

        if "type" not in schema or schema["type"] != "object":
            raise ValueError("Schema type has to be 'object'")

        tables = self._process_schema(schema)
        for table in tables:
            yield table

    def _get_schema(self):
        if self.schema_path.startswith(self.S3_PROTOCOL):
            schema = self._read_file_from_s3(self.schema_path)
        else:
            schema = read_json(self.schema_path)

        return schema

    def _process_schema(
        self, schema: dict, column_prefix: str = ""
    ) -> Iterable[TableMetadata]:
        if self.pivot_column:
            if "oneOf" not in schema:
                raise ValueError(
                    "Pivot column provided, but no top-level 'oneOf' in schema"
                )
            schemas = {}
            for constraint in schema["oneOf"]:
                try:
                    subschema_name = str(
                        constraint["properties"][self.pivot_column]["const"]
                    )
                except KeyError:
                    raise ValueError("Pivot column inside oneOf should be a const")

                merged_schema = self._deep_merge_dicts(
                    constraint, copy.deepcopy(schema)
                )
                schemas[subschema_name] = merged_schema

        else:
            schemas = {self.normalise(schema.get("title", "schema")): schema}

        return [self._schema_to_table(name, schema) for name, schema in schemas.items()]

    def _deep_merge_dicts(self, source: dict, destination: dict):
        for key, value in source.items():
            if isinstance(value, dict):
                # get node or create one
                node = destination.setdefault(key, {})
                self._deep_merge_dicts(value, node)
            elif (
                isinstance(value, list)
                and key in destination
                and isinstance(destination[key], list)
            ):
                destination[key] += value
            else:
                destination[key] = value

        return destination

    def _schema_to_table(self, name: str, schema: dict) -> TableMetadata:
        required_columns = schema.get("required", [])
        columns = {}
        for key, val in schema.get("properties").items():
            columns[key] = val

            if self.object_expand and key in self.object_expand:
                for subkey, subval in val.get("properties", {}).items():
                    columns[f"{key}.{subkey}"] = subval

        mapped_columns = [
            self._process_column(column_name, column_def, required_columns)
            for column_name, column_def in columns.items()
            if (
                self.filter_columns.search(column_name) if self.filter_columns else True
            )
        ]

        if self.pin_column:
            mapped_columns = self._pin_columns(mapped_columns)

        description = (
            schema.get(self.DESCRIPTION_KEY) if self.extract_descriptions else None
        )

        if self.tags_key is not None:
            raw_tags = schema

            for key_segment in self.tags_key:
                raw_tags = raw_tags.get(key_segment, {})
        else:
            raw_tags = {}

        tags = [TableTag(key, value) for key, value in raw_tags.items()]

        return TableMetadata(
            name=name,
            description=description,
            database=self.database,
            connection=self.connection_name,
            location=self.schema_path,
            columns=mapped_columns,
            table_type=TableType.TABLE,
            tags=tags,
        )

    def _pin_columns(
        self, mapped_columns: List[ColumnMetadata]
    ) -> List[ColumnMetadata]:

        pinned_columns = [
            column for column in mapped_columns if self.pin_column.search(column.name)
        ]
        regular_columns = [
            column
            for column in mapped_columns
            if not self.pin_column.search(column.name)
        ]

        return pinned_columns + regular_columns

    def _process_column(
        self, column_name: str, column_def: dict, required_columns: List[str]
    ):
        is_required = column_name in required_columns
        column_type = column_def.get("type", "") + (
            " (required)" if is_required else ""
        )
        column_values = column_def.get("enum")
        column_description = (
            column_def.get(self.DESCRIPTION_KEY) if self.extract_descriptions else None
        )
        return ColumnMetadata(
            name=column_name,
            column_type=column_type,
            values=column_values,
            description=column_description,
        )

    def _read_file_from_s3(self, path):
        path_parts_without_protocol = path[len(self.S3_PROTOCOL) :].split("/")
        bucket = path_parts_without_protocol[0]
        key = "/".join(path_parts_without_protocol[1:])
        content_object = self.s3.Object(bucket, key)

        file_content = content_object.get()["Body"].read().decode("utf-8")
        json_content = json.loads(file_content)

        return json_content

    def normalise(self, value: str):
        return value.replace("-", "_").replace(" ", "_").lower()
