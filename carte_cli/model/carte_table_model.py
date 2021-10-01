from enum import Enum
from typing import Any, List, Union
from databuilder.models.table_metadata import TableMetadata as DatabuilderTableMetadata
from databuilder.models.table_metadata import (
    ColumnMetadata as DatabuilderColumnMetadata,
)
from databuilder.models.table_metadata import (
    DescriptionMetadata as DatabuilderDescription,
)


def get_description_text(description: DatabuilderDescription):
    if hasattr(description, "text"):
        return description.text


class ColumnType(Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOL = "bool"
    DATE = "date"
    BINARY = "binary"


class TableType(Enum):
    TABLE = "table"
    VIEW = "view"


class ColumnMetadata:
    def __init__(
        self,
        name: str,
        column_type: str,
        description: str = None,
        values: Union[None, List[Any]] = None,
        example_value: str = None,
    ):
        self.name = name
        self.column_type = column_type
        self.description = description
        self.values = values
        self.example_value = example_value

    @classmethod
    def from_databuilder(cls, column: DatabuilderColumnMetadata):
        return cls(
            name=column.name,
            description=(
                get_description_text(column.description)
                if column.description is not None
                else ""
            ),
            column_type=column.type,
        )

    @classmethod
    def from_frontmatter(cls, meta_dict):
        return cls(
            name=meta_dict["name"],
            column_type=meta_dict.get("type"),
            description=meta_dict.get("description"),
            values=meta_dict.get("values"),
            example_value=meta_dict.get("example"),
        )

    def to_frontmatter(self):
        frontmatter = {
            "name": self.name,
            "type": self.column_type,
            "description": self.description,
            "example": self.example_value,
        }

        if self.values is not None:
            frontmatter["values"] = self.values

        return frontmatter

    def __repr__(self) -> str:
        return "CarteTableMetadata({!r}, {!r}, {!r}, {!r}, {!r})".format(
            self.name,
            self.column_type,
            self.description,
            self.values,
            self.example_value,
        )


class TableTag:
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value

    @classmethod
    def from_frontmatter(cls, meta_dict: dict):
        return cls(key=meta_dict["key"], value=meta_dict["value"])

    def to_frontmatter(self):
        return {"key": self.key, "value": self.value}

    def __repr__(self) -> str:
        return "TableTag({!r}: {!r})".format(self.key, self.value)


class TableMetadata:
    def __init__(
        self,
        name: str,
        database: str,
        connection: str,
        location: str,
        columns: List[ColumnMetadata],
        table_type: TableType,
        tags: List[TableTag] = [],
        description: str = None,
    ):
        self.name = name
        self.connection = connection
        self.database = database
        self.description = description
        self.location = location
        self.columns = columns
        self.tags = tags
        self.table_type = table_type

    @classmethod
    def from_databuilder(cls, table: DatabuilderTableMetadata):
        columns = [ColumnMetadata.from_databuilder(col) for col in table.columns]

        return cls(
            name=table.name,
            location=table._get_table_key(),
            database=table.cluster,
            connection=table.database,
            description=(
                get_description_text(table.description)
                if table.description is not None
                else None
            ),
            columns=columns,
            tags=[],
            table_type=(TableType.VIEW if table.is_view else TableType.TABLE),
        )

    @classmethod
    def from_frontmatter(cls, metadata, content):
        columns = [
            ColumnMetadata.from_frontmatter(col_dict)
            for col_dict in metadata.get("columns", [])
        ]

        tags = [TableTag.from_frontmatter(tag) for tag in metadata.get("tags", [])]

        try:
            return cls(
                name=metadata["title"],
                database=metadata.get("database", None),
                description=content,
                location=metadata.get("location", None),
                connection=metadata.get("connection", None),
                columns=columns,
                tags=tags,
                table_type=TableType(metadata.get("table_type", "table")),
            )
        except KeyError as e:
            raise ValueError(f"Key not found in frontmatter: {e}. Metadata: {metadata}")

    def to_frontmatter(self):
        metadata = {
            "title": self.name,
            "connection": self.connection,
            "location": self.location,
            "database": self.database,
            "columns": [col.to_frontmatter() for col in self.columns],
            "tags": [tag.to_frontmatter() for tag in self.tags],
            "table_type": self.table_type.value,
        }
        return metadata, self.description

    def get_columns_by_name(self):
        return {col.name: col for col in self.columns}

    def get_tags_by_name(self):
        return {tag.key: tag.value for tag in self.tags}

    def merge_columns(self, existing, preserve_descriptions=True):
        self_columns_dict = self.get_columns_by_name()
        existing_columns_dict = existing.get_columns_by_name()

        merged_columns = []

        for column_name, column in self_columns_dict.items():
            merged_description = (
                existing_columns_dict[column_name].description
                if column_name in existing_columns_dict
                else column.description
            )
            merged_example = (
                existing_columns_dict[column_name].example_value
                if column_name in existing_columns_dict
                else column.example_value
            )

            merged_values = (
                existing_columns_dict[column_name].values
                if column_name in existing_columns_dict and column.values is None
                else column.values
            )

            merged_columns.append(
                ColumnMetadata(
                    name=column_name,
                    column_type=column.column_type,
                    description=merged_description,
                    values=merged_values,
                    example_value=merged_example,
                )
            )
        return merged_columns

    def merge_tags(self, existing):
        self_tags_dict = self.get_tags_by_name()
        existing_tags_dict = existing.get_tags_by_name()

        merged_tags = existing_tags_dict.copy()

        for key, value in self_tags_dict.items():
            merged_tags[key] = value

        return [TableTag(key=key, value=value) for key, value in merged_tags.items()]

    def merge_with_existing(self, existing):
        if existing.name is not None and existing.name != self.name:
            raise ValueError("Table names not equal!")

        if existing.database is not None and existing.database != self.database:
            raise ValueError(
                f"Database not equal! Existing: {existing.database}, new: {self.database}"
            )

        description = existing.description if not self.description else self.description

        return TableMetadata(
            name=self.name,
            database=self.database,
            connection=self.connection,
            description=description,
            location=self.location,
            columns=self.merge_columns(existing),
            tags=self.merge_tags(existing),
            table_type=self.table_type,
        )

    def get_file_name(self):
        return f"{self.connection}/{self.database}/{self.name}"

    def __repr__(self) -> str:
        return (
            "CarteTableMetadata({!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r})".format(
                self.name,
                self.database,
                self.connection,
                self.description,
                self.location,
                self.columns,
                self.tags,
                self.table_type,
            )
        )
