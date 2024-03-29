import unittest
from unittest.mock import patch

from carte_cli.model.carte_table_model import TableMetadata, ColumnMetadata, TableType
from databuilder.models.table_metadata import ColumnMetadata as DatabuilderColumn
from databuilder.models.table_metadata import (
    DescriptionMetadata as DatabuilderDescription,
)
from databuilder.models.table_metadata import TableMetadata as DatabuilderTable


class TestColumnMetadata(unittest.TestCase):
    def test_from_databuilder(self):
        source_metadata = DatabuilderColumn(
            "test-name", "test-description", "test-type", 1
        )

        result = ColumnMetadata.from_databuilder(source_metadata)

        assert result.name == "test-name"
        assert result.column_type == "test-type"
        assert result.description == "test-description"

    def test_from_frontmatter(self):
        source_metadata = {
            "name": "test-name",
            "description": "test-description",
            "type": "test-type",
            "example": 2020,
        }

        result = ColumnMetadata.from_frontmatter(source_metadata)

        assert result.name == "test-name"
        assert result.description == "test-description"
        assert result.column_type == "test-type"
        assert result.example_value == 2020

    def test_from_frontmatter_no_values(self):
        source_metadata = {"name": "test-name"}

        result = ColumnMetadata.from_frontmatter(source_metadata)

        assert result.name == "test-name"
        assert result.description is None
        assert result.column_type is None

    def test_from_frontmatter_raises_with_no_name(self):
        self.assertRaises(KeyError, ColumnMetadata.from_frontmatter, {})

    def test_to_frontmatter(self):
        source = ColumnMetadata(
            name="test-name",
            column_type="test-type",
            description="test-description",
            example_value="test-example",
        )

        result = source.to_frontmatter()

        assert result == {
            "name": "test-name",
            "type": "test-type",
            "description": "test-description",
            "example": "test-example",
        }


class TestTableMetadata(unittest.TestCase):
    def test_from_databuilder(self):
        source_metadata = DatabuilderTable(
            "test-connection",
            "test-cluster",
            "test-db",
            "test-name",
            "test-description",
            [
                DatabuilderColumn("test-col-1", "test-descr1", "test-type1", 1),
                DatabuilderColumn("test-col-2", "test-descr2", "test-type2", 2),
            ],
            False,
        )

        result = TableMetadata.from_databuilder(source_metadata)

        assert result.database == "test-cluster"
        assert result.location == "test-connection://test-cluster.test-db/test-name"
        assert result.connection == "test-connection"
        assert len(result.tags) == 0
        assert len(result.columns) == 2
        assert (
            result.columns[0].__repr__()
            == ColumnMetadata(
                name="test-col-1", description="test-descr1", column_type="test-type1"
            ).__repr__()
        )
        assert (
            result.columns[1].__repr__()
            == ColumnMetadata(
                name="test-col-2", description="test-descr2", column_type="test-type2"
            ).__repr__()
        )
        assert result.table_type == TableType.TABLE

    def test_from_frontmatter(self):
        source_metadata = {
            "title": "test-name",
            "database": "test-db",
            "location": "test-location",
            "connection": "test-connection",
            "columns": [
                {
                    "name": "column-a",
                    "type": "test-type",
                    "example": "test-example",
                    "description": "test-description",
                }
            ],
            "tags": [{"key": "a", "value": "val1"}, {"key": "b", "value": "val2"}],
            "table_type": "table",
        }

        result = TableMetadata.from_frontmatter(source_metadata, "test-description")

        assert result.name == "test-name"
        assert result.database == "test-db"
        assert result.location == "test-location"
        assert result.connection == "test-connection"
        assert (
            result.columns[0].__repr__()
            == ColumnMetadata(
                "column-a",
                "test-type",
                "test-description",
                example_value="test-example",
            ).__repr__()
        )
        assert len(result.columns) == 1
        assert result.table_type == TableType.TABLE
        assert result.tags[0].key == "a"
        assert result.tags[0].value == "val1"
        assert result.tags[1].key == "b"
        assert result.tags[1].value == "val2"

    def test_from_frontmatter_no_values(self):
        source_metadata = {"title": "test-name"}

        result = TableMetadata.from_frontmatter(source_metadata, None)

        assert result.name == "test-name"
        assert result.description is None
        assert result.columns == []

    def test_from_frontmatter_raises_with_no_name(self):
        self.assertRaises(ValueError, TableMetadata.from_frontmatter, {}, None)

    def test_to_frontmatter(self):
        source = TableMetadata(
            name="test-name",
            database="test-db",
            description="test-description",
            location="test-location",
            connection="test-connection",
            columns=[],
            table_type=TableType.VIEW,
        )

        metadata, content = source.to_frontmatter()

        assert content == "test-description"
        assert metadata["title"] == "test-name"
        assert metadata["connection"] == "test-connection"
        assert metadata["location"] == "test-location"
        assert metadata["database"] == "test-db"
        assert metadata["columns"] == []
        assert metadata["table_type"] == "view"
