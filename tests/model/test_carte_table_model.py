import unittest
from unittest.mock import patch

from flyover.model.carte_table_model import TableMetadata, ColumnMetadata, TableType
from databuilder.models.table_metadata import ColumnMetadata as DatabuilderColumn
from databuilder.models.table_metadata import DescriptionMetadata as DatabuilderDescription
from databuilder.models.table_metadata import TableMetadata as DatabuilderTable



class TestColumnMetadata(unittest.TestCase):
    def test_from_databuilder(self):
        source_metadata = DatabuilderColumn(
            "test-name",
            "test-description",
            "test-type",
            1
        )

        result = ColumnMetadata.from_databuilder(source_metadata)

        assert result.name == "test-name"
        assert result.column_type == "test-type"
        assert result.description == "test-description"

    def test_from_frontmatter(self):
        source_metadata = {
            "name": "test-name",
            "description": "test-description",
            "type": "test-type"
        }

        result = ColumnMetadata.from_frontmatter(source_metadata)

        assert result.name == "test-name"
        assert result.description == "test-description"
        assert result.column_type == "test-type"

    def test_from_frontmatter_no_values(self):
        source_metadata = {
            "name": "test-name"
        }

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
            description="test-description"
        )

        result = source.to_frontmatter()

        assert result == {
            "name": "test-name",
            "type": "test-type",
            "description": "test-description"
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
            False
        )

        result = TableMetadata.from_databuilder(source_metadata)

        assert result.database == "test-db"
        assert result.location == "test-connection://test-cluster.test-db/test-name"
        assert result.connection == "test-connection"
        assert len(result.columns) == 2
        assert result.columns[0].__repr__() == ColumnMetadata(name="test-col-1", description="test-descr1", column_type="test-type1").__repr__()
        assert result.columns[1].__repr__() == ColumnMetadata(name="test-col-2", description="test-descr2", column_type="test-type2").__repr__()
        assert result.table_type == TableType.TABLE

    def test_from_frontmatter(self):
        pass

    def test_from_frontmatter_no_values(self):
        pass

    def test_from_frontmatter_raises_with_no_name(self):
        pass

    def test_to_frontmatter(self):
        pass
