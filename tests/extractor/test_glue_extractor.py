import boto3
import unittest
from unittest.mock import patch
from pyhocon import ConfigFactory

from flyover.extractor.glue_extractor import GlueExtractor
from flyover.model.carte_table_model import TableMetadata, ColumnMetadata, TableType


@patch("flyover.extractor.glue_extractor.boto3.client", lambda x: None)
class TestGlueExtractor(unittest.TestCase):
    def setUp(self) -> None:
        self.conf = ConfigFactory.from_dict({})

    def test_extraction_with_empty_query_result(self) -> None:

        with patch.object(GlueExtractor, '_search_tables'):
            extractor = GlueExtractor()
            extractor.init(self.conf)

            results = extractor.extract()
            self.assertEqual(results, None)

    def test_extraction_with_single_result(self) -> None:
        with patch.object(GlueExtractor, '_search_tables') as mock_search:
            mock_search.return_value = [
                {
                    'Name': 'test_table',
                    'DatabaseName': 'test_schema',
                    'Description': 'a table for testing',
                    'StorageDescriptor': {
                        'Columns': [
                            {
                                'Name': 'col_id1',
                                'Type': 'bigint',
                                'Comment': 'description of id1'
                            },
                            {
                                'Name': 'col_id2',
                                'Type': 'bigint',
                                'Comment': 'description of id2'
                            },
                            {
                                'Name': 'is_active',
                                'Type': 'boolean'
                            },
                            {
                                'Name': 'source',
                                'Type': 'varchar',
                            },
                            {
                                'Name': 'etl_created_at',
                                'Type': 'timestamp',
                                'Comment': 'description of etl_created_at'
                            },
                        ],
                        'Location': 'test_location'
                    },
                    'PartitionKeys': [
                        {
                            'Name': 'partition_key1',
                            'Type': 'string',
                            'Comment': 'description of partition_key1'
                        },
                    ],
                    'TableType': 'EXTERNAL_TABLE',
                }
            ]

            extractor = GlueExtractor()
            extractor.init(self.conf)
            actual = extractor.extract()
            expected = TableMetadata(
                name='test_table',
                connection='glue',
                database='test_schema',
                description=None,
                location='test_location',
                table_type=TableType.TABLE,
                columns=[
                    ColumnMetadata('col_id1', 'bigint', None),
                    ColumnMetadata('col_id2', 'bigint', None),
                    ColumnMetadata('is_active', 'boolean', None),
                    ColumnMetadata('source', 'varchar', None),
                    ColumnMetadata('etl_created_at', 'timestamp', None),
                    ColumnMetadata('partition_key1', 'string', None)
                ]
            )
            self.assertEqual(expected.__repr__(), actual.__repr__())
            self.assertIsNone(extractor.extract())

    def test_extraction_with_multiple_result(self) -> None:
        with patch.object(GlueExtractor, '_search_tables') as mock_search:
            mock_search.return_value = [
                {
                    'Name': 'test_table1',
                    'DatabaseName': 'test_schema1',
                    'Description': 'test table 1',
                    'StorageDescriptor': {
                        'Columns': [
                            {
                                'Name': 'col_id1',
                                'Type': 'bigint',
                                'Comment': 'description of col_id1'
                            },
                            {
                                'Name': 'source',
                                'Type': 'varchar',
                                'Comment': 'description of source'
                            },
                            {
                                'Name': 'ds',
                                'Type': 'varchar'
                            }
                        ]
                    },
                    'PartitionKeys': [
                        {
                            'Name': 'partition_key1',
                            'Type': 'string',
                            'Comment': 'description of partition_key1'
                        },
                    ],
                    'TableType': 'EXTERNAL_TABLE',
                },
                {
                    'Name': 'test_table2',
                    'DatabaseName': 'test_schema1',
                    'Description': 'test table 2',
                    'StorageDescriptor': {
                        'Columns': [
                            {
                                'Name': 'col_name',
                                'Type': 'varchar',
                                'Comment': 'description of col_name'
                            },
                            {
                                'Name': 'col_name2',
                                'Type': 'varchar',
                                'Comment': 'description of col_name2'
                            }
                        ],
                        'Location': 'test_location2'
                    },
                    'TableType': 'EXTERNAL_TABLE',
                },
                {
                    'Name': 'test_view1',
                    'DatabaseName': 'test_schema1',
                    'Description': 'test view 1',
                    'StorageDescriptor': {
                        'Columns': [
                            {
                                'Name': 'col_id3',
                                'Type': 'varchar',
                                'Comment': 'description of col_id3'
                            },
                            {
                                'Name': 'col_name3',
                                'Type': 'varchar',
                                'Comment': 'description of col_name3'
                            }
                        ]
                    },
                    'TableType': 'VIRTUAL_VIEW',
                },
            ]

            extractor = GlueExtractor()
            extractor.init(self.conf)

            expected = TableMetadata(
                name='test_table1',
                connection='glue',
                database='test_schema1',
                description=None,
                location=None,
                table_type=TableType.TABLE,
                columns=[
                    ColumnMetadata('col_id1', 'bigint', None),
                    ColumnMetadata('source', 'varchar', None),
                    ColumnMetadata('ds', 'varchar', None),
                    ColumnMetadata('partition_key1', 'string', None)
                ]
            )

            self.assertEqual(expected.__repr__(), extractor.extract().__repr__())

            expected = TableMetadata(
                name='test_table2',
                connection='glue',
                database='test_schema1',
                description=None,
                location='test_location2',
                table_type=TableType.TABLE,
                columns=[
                    ColumnMetadata('col_name', 'varchar', None),
                    ColumnMetadata('col_name2', 'varchar', None),
                ]
            )

            self.assertEqual(expected.__repr__(), extractor.extract().__repr__())

            expected = TableMetadata(
                name='test_view1',
                connection='glue',
                database='test_schema1',
                description=None,
                location=None,
                table_type=TableType.VIEW,
                columns=[
                    ColumnMetadata('col_id3', 'varchar', None),
                    ColumnMetadata('col_name3', 'varchar', None)
                ]
            )

            self.assertEqual(expected.__repr__(), extractor.extract().__repr__())

            self.assertIsNone(extractor.extract())
            self.assertIsNone(extractor.extract())
