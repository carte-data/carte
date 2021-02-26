import os
import unittest
from unittest.mock import patch
import pytest

from pyhocon import ConfigFactory
from typing import Dict, Iterable, Any, Callable  # noqa: F401

from carte_cli.loader.carte_loader import CarteLoader
from carte_cli.model.carte_table_model import TableMetadata, ColumnMetadata, TableType


@pytest.fixture
def patched_config():
    return ConfigFactory.from_dict(
        {"base_directory": ".", "tables_output_path": "tables"}
    )


@patch("carte_cli.loader.carte_loader.frontmatter")
@patch("carte_cli.loader.carte_loader.os")
def test_load_carte_metadata(mock_os, mock_frontmatter, patched_config):
    test_record = TableMetadata(
        name="test_name",
        database="test_db1",
        location="test_loc",
        connection="glue",
        description=None,
        table_type=TableType.TABLE,
        columns=[
            ColumnMetadata("test_col1", "string", None),
            ColumnMetadata("test_col2", "bigint", None),
        ],
    )

    mock_os.path.isfile.return_value = False
    mock_os.path.join.return_value = "mock_path"

    loader = CarteLoader()
    loader.init(patched_config)
    loader.load(test_record)

    loader.close()

    mock_frontmatter.dump.assert_called_with("mock_path", *test_record.to_frontmatter())
    mock_os.path.join.assert_called_with(
        ".", "tables", f"{test_record.get_file_name()}.md"
    )


@patch("carte_cli.loader.carte_loader.frontmatter")
@patch("carte_cli.loader.carte_loader.os")
def test_load_carte_metadata_with_merge(mock_os, mock_frontmatter, patched_config):
    test_record = TableMetadata(
        name="test_name",
        database="test_db1",
        location="test_loc",
        connection="glue",
        description=None,
        table_type=TableType.TABLE,
        columns=[
            ColumnMetadata("test_col1", "string", None),
            ColumnMetadata("test_col2", "bigint", None),
        ],
    )

    existing_record = TableMetadata(
        name="test_name",
        database="test_db1",
        location="old_loc",
        connection="glue",
        description="Something",
        table_type=TableType.TABLE,
        columns=[
            ColumnMetadata("test_col1", "string", None),
            ColumnMetadata("test_col2", "varchar", None)
        ]
    )

    final_record = test_record.merge_with_existing(existing_record)

    mock_os.path.isfile.return_value = True
    mock_os.path.join.return_value = "mock_path"
    mock_frontmatter.parse.return_value = existing_record.to_frontmatter()
    existing_frontmatter = existing_record.to_frontmatter()


    loader = CarteLoader()
    loader.init(patched_config)
    loader.load(test_record)

    loader.close()

    mock_frontmatter.dump.assert_called_with("mock_path", *final_record.to_frontmatter())
