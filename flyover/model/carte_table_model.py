from enum import Enum
from typing import Any, List
from databuilder.models.table_metadata import TableMetadata as DatabuilderTableMetadata
from databuilder.models.table_metadata import (
    ColumnMetadata as DatabuilderColumnMetadata,
)
from databuilder.models.table_metadata import (
    DescriptionMetadata as DatabuilderDescription,
)


def get_description_text(description: DatabuilderDescription):
    return description._text


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
    def __init__(self, name: str, column_type: str, description: str):
        self.name = name
        self.column_type = column_type
        self.description = description

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
        )

    def to_frontmatter(self):
        return {
            "name": self.name,
            "type": self.column_type,
            "description": self.description,
        }

    def __repr__(self) -> str:
        return "CarteTableMetadata({!r}, {!r}, {!r})".format(
            self.name,
            self.column_type,
            self.description,
        )


class TableMetadata:
    def __init__(
        self,
        name: str,
        database: str,
        connection: str,
        description: str,
        location: str,
        columns: List[ColumnMetadata],
        table_type: TableType,
    ):
        self.name = name
        self.connection = connection
        self.database = database
        self.description = description
        self.location = location
        self.columns = columns
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
            table_type=(TableType.VIEW if table.is_view else TableType.TABLE),
        )

    @classmethod
    def from_frontmatter(cls, metadata, content):
        columns = [
            ColumnMetadata.from_frontmatter(col_dict)
            for col_dict in metadata.get("columns", [])
        ]

        try:
            return cls(
                name=metadata["title"],
                database=metadata.get("database", None),
                description=content,
                location=metadata.get("location", None),
                connection=metadata.get("connection", None),
                columns=columns,
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
            "table_type": self.table_type.value,
        }
        return metadata, self.description

    def get_columns_by_name(self):
        return {col.name: col for col in self.columns}

    def merge_columns(self, existing, preserve_descriptions=True):
        self_columns_dict = self.get_columns_by_name()
        existing_columns_dict = existing.get_columns_by_name()

        merged_columns = []

        for column_name, column in self_columns_dict.items():
            merged_description = (
                existing_columns_dict[column_name].description
                if preserve_descriptions and column_name in existing_columns_dict
                else column.description
            )
            merged_columns.append(
                ColumnMetadata(
                    name=column_name,
                    column_type=column.column_type,
                    description=merged_description,
                )
            )
        return merged_columns

    def merge_with_existing(self, existing):
        if existing.name is not None and existing.name != self.name:
            raise ValueError("Table names not equal!")

        if existing.database is not None and existing.database != self.database:
            raise ValueError(
                f"Database not equal! Existing: {existing.database}, new: {self.database}"
            )

        return TableMetadata(
            name=self.name,
            database=self.database,
            connection=self.connection,
            description=existing.description,
            location=self.location,
            columns=self.merge_columns(existing),
            table_type=self.table_type,
        )

    def get_file_name(self):
        return f"{self.connection}/{self.database}/{self.name}"

    def __repr__(self) -> str:
        return "CarteTableMetadata({!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r})".format(
            self.name,
            self.database,
            self.connection,
            self.description,
            self.location,
            self.columns,
            self.table_type,
        )
