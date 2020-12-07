import boto3
import unittest
from unittest.mock import patch
from pyhocon import ConfigFactory

from flyover.extractor.glue_extractor import GlueExtractor
from flyover.models.carte_table_model import TableMetadata

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
                }
            ]

            extractor = GlueExtractor()
            extractor.init(self.conf)
            actual = extractor.extract()
            expected = TableMetadata(
                name=
            )
            expected = TableMetadata('glue', 'gold', 'test_schema', 'test_table', 'a table for testing',
                                     [ColumnMetadata('col_id1', 'description of id1', 'bigint', 0),
                                      ColumnMetadata('col_id2', 'description of id2', 'bigint', 1),
                                      ColumnMetadata('is_active', None, 'boolean', 2),
                                      ColumnMetadata('source', 'description of source', 'varchar', 3),
                                      ColumnMetadata('etl_created_at', 'description of etl_created_at', 'timestamp', 4),
                                      ColumnMetadata('ds', None, 'varchar', 5),
                                      ColumnMetadata('partition_key1', 'description of partition_key1', 'string', 6),
                                      ], False)
            self.assertEqual(expected.__repr__(), actual.__repr__())
            self.assertIsNone(extractor.extract())
