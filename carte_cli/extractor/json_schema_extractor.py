from typing import List, Union, Iterator, Any, Iterable, Dict
from carte_cli.model.carte_table_model import TableMetadata, ColumnMetadata, TableType
from carte_cli.utils.file_io import read_json
from databuilder.extractor.base_extractor import Extractor
from pyhocon import ConfigTree
import boto3
import json
import copy


class JSONSchemaExtractor(Extractor):

    SCHEMA_PATH_KEY = "schema_path"
    S3_PROTOCOL = "s3://"

    def __init__(
        self,
        connection_name: str,
        database: str,
        schema_path: str,
        pivot_column: str = None,
        object_expand: Iterable[str] = None,
    ):
        super().__init__()
        self.connection_name = connection_name
        self.database = database
        self.schema_path = schema_path
        self.s3 = boto3.resource("s3")
        self.pivot_column = pivot_column
        self.object_expand = object_expand
        self._extract_iter = iter(self._get_extract_iter())

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf

    def extract(self) -> Any:
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        return "carte.extractor.json_schema"

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
                    subschema_name = str(constraint["properties"][self.pivot_column][
                        "const"
                    ])
                except KeyError:
                    raise ValueError("Pivot column inside oneOf should be a const")

                merged_schema = self._deep_merge_dicts(constraint, copy.deepcopy(schema))
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
        ]

        return TableMetadata(
            name=name,
            database=self.database,
            connection=self.connection_name,
            location=self.schema_path,
            columns=mapped_columns,
            table_type=TableType.TABLE,
        )

    def _process_column(
        self, column_name: str, column_def: dict, required_columns: List[str]
    ):
        is_required = column_name in required_columns
        column_type = column_def.get("type", "") + (
            " (required)" if is_required else ""
        )
        column_values = column_def.get("enum", None)
        return ColumnMetadata(
            name=column_name, column_type=column_type, values=column_values
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
