import boto3
import unittest
from unittest.mock import patch
from pyhocon import ConfigFactory
from pyhocon.config_tree import ConfigTree

from carte_cli.extractor.json_schema_extractor import JSONSchemaExtractor
from carte_cli.model.carte_table_model import (
    TableMetadata,
    ColumnMetadata,
    TableTag,
    TableType,
)


class TestJSONSchemaExtractor(unittest.TestCase):
    TEST_DATABASE = "test-database"

    def get_conf(self, additional_config: dict = {}) -> ConfigTree:
        return ConfigFactory.from_dict(
            {
                JSONSchemaExtractor.SCHEMA_PATH_KEY: "test-schema-path",
                **additional_config,
            }
        )

    def test_extraction_with_no_columns(self) -> None:
        with patch.object(JSONSchemaExtractor, "_get_schema") as mock_get_schema:
            mock_get_schema.return_value = {
                "type": "object",
                "title": "test-schema",
                "properties": {},
            }
            extractor = JSONSchemaExtractor("test-connection", self.TEST_DATABASE)
            extractor.init(self.get_conf())

            results = extractor.extract()
            self.assertEqual(results.connection, "test-connection")
            self.assertEqual(results.database, self.TEST_DATABASE)
            self.assertEqual(results.columns, [])

    def test_extraction_raises_with_no_type(self) -> None:
        with patch.object(JSONSchemaExtractor, "_get_schema") as mock_get_schema:
            mock_get_schema.return_value = {}
            extractor = JSONSchemaExtractor("test-connection", self.TEST_DATABASE)
            extractor.init(self.get_conf())

            with self.assertRaises(ValueError):
                results = extractor.extract()

    def test_extraction_with_simple_schema(self) -> None:
        with patch.object(JSONSchemaExtractor, "_get_schema") as mock_get_schema:
            mock_get_schema.return_value = {
                "type": "object",
                "title": "Test schema",
                "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
                "required": ["name"],
            }
            extractor = JSONSchemaExtractor("test-connection", self.TEST_DATABASE)
            extractor.init(self.get_conf())

            results = extractor.extract()
            self.assertEqual(results.connection, "test-connection")
            self.assertEqual(results.database, self.TEST_DATABASE)
            self.assertEqual(results.name, "test_schema")
            self.assertEqual(results.columns[0].name, "name")
            self.assertEqual(results.columns[0].column_type, "string (required)")
            self.assertEqual(results.columns[0].description, None)

            self.assertEqual(results.columns[1].name, "age")
            self.assertEqual(results.columns[1].column_type, "integer")
            self.assertEqual(results.columns[1].description, None)

            results = extractor.extract()
            self.assertEqual(results, None)

    def test_raises_with_pivot_and_no_oneof(self) -> None:
        with patch.object(JSONSchemaExtractor, "_get_schema") as mock_get_schema:
            mock_get_schema.return_value = {
                "type": "object",
                "title": "test-schema",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                    "glasses": {"type": "boolean"},
                },
                "required": ["name"],
            }

            extractor = JSONSchemaExtractor("test-connection", self.TEST_DATABASE)
            extractor.init(self.get_conf({"pivot_column": "glasses"}))

            with self.assertRaises(ValueError):
                results = extractor.extract()

    def test_raises_if_pivot_is_not_const(self) -> None:
        with patch.object(JSONSchemaExtractor, "_get_schema") as mock_get_schema:
            mock_get_schema.return_value = {
                "type": "object",
                "title": "test-schema",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                    "glasses": {"type": "boolean"},
                },
                "required": ["name"],
                "oneOf": [
                    {
                        "properties": {
                            "glasses": {"type": "string"},
                            "focal_length": {"type": "float"},
                        },
                        "required": ["focal_length"],
                    }
                ],
            }
            extractor = JSONSchemaExtractor("test-connection", self.TEST_DATABASE)
            extractor.init(self.get_conf({"pivot_column": "glasses"}))

            with self.assertRaises(ValueError):
                results = extractor.extract()

    def test_extracts_with_pivot(self) -> None:
        with patch.object(JSONSchemaExtractor, "_get_schema") as mock_get_schema:
            mock_get_schema.return_value = {
                "type": "object",
                "title": "test-schema",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                    "glasses": {"type": "boolean"},
                },
                "required": ["name"],
                "oneOf": [
                    {
                        "properties": {
                            "glasses": {"const": True},
                            "focal_length": {"type": "float"},
                        },
                        "required": ["focal_length"],
                    },
                    {
                        "properties": {
                            "glasses": {"const": False},
                        },
                    },
                ],
            }

            extractor = JSONSchemaExtractor("test-connection", self.TEST_DATABASE)
            extractor.init(self.get_conf({"pivot_column": "glasses"}))

            results = extractor.extract()
            self.assertEqual(results.connection, "test-connection")
            self.assertEqual(results.database, "test-database")
            self.assertEqual(results.location, "test-schema-path")
            self.assertEqual(results.name, "True")
            self.assertEqual(results.columns[3].name, "focal_length")
            self.assertEqual(results.columns[3].column_type, "float (required)")

            results = extractor.extract()
            self.assertEqual(results.connection, "test-connection")
            self.assertEqual(results.database, "test-database")
            self.assertEqual(results.location, "test-schema-path")
            self.assertEqual(results.name, "False")
            self.assertEqual(len(results.columns), 3)

            results = extractor.extract()
            self.assertEqual(results, None)

    def test_expands_top_level_object(self) -> None:
        with patch.object(JSONSchemaExtractor, "_get_schema") as mock_get_schema:
            mock_get_schema.return_value = {
                "type": "object",
                "title": "test-schema",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                    "test-props": {
                        "type": "object",
                        "properties": {
                            "person-prop-1": {"type": "string"},
                            "person-prop-2": {"type": "integer"},
                        },
                    },
                },
            }

            extractor = JSONSchemaExtractor("test-connection", self.TEST_DATABASE)
            extractor.init(self.get_conf({"object_expand": ["test-props"]}))

            results = extractor.extract()
            self.assertEqual(len(results.columns), 5)
            self.assertEqual(
                results.columns[2].__repr__(),
                ColumnMetadata("test-props", "object").__repr__(),
            )
            self.assertEqual(
                results.columns[3].__repr__(),
                ColumnMetadata("test-props.person-prop-1", "string").__repr__(),
            )
            self.assertEqual(
                results.columns[4].__repr__(),
                ColumnMetadata("test-props.person-prop-2", "integer").__repr__(),
            )

    def test_expands_pivoted_object(self) -> None:
        with patch.object(JSONSchemaExtractor, "_get_schema") as mock_get_schema:
            mock_get_schema.return_value = {
                "type": "object",
                "title": "test-schema",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                    "test-props": {
                        "type": "object",
                        "properties": {
                            "person-prop-1": {"type": "string"},
                            "person-prop-2": {"type": "integer"},
                        },
                    },
                },
                "oneOf": [
                    {
                        "properties": {
                            "name": {"const": "John"},
                            "test-props": {
                                "properties": {"person-prop-3": {"type": "float"}}
                            },
                        }
                    },
                    {
                        "properties": {
                            "name": {"const": "Mary"},
                            "test-props": {
                                "properties": {"person-prop-4": {"type": "float"}}
                            },
                        }
                    },
                ],
            }

            extractor = JSONSchemaExtractor("test-connection", self.TEST_DATABASE)
            extractor.init(
                self.get_conf(
                    {
                        "object_expand": ["test-props"],
                        "pivot_column": "name",
                    }
                )
            )

            results = extractor.extract()
            self.assertEqual(results.name, "John")
            self.assertEqual(len(results.columns), 6)
            self.assertEqual(
                results.columns[5].__repr__(),
                ColumnMetadata("test-props.person-prop-3", "float").__repr__(),
            )

            results = extractor.extract()
            self.assertEqual(results.name, "Mary")
            self.assertEqual(len(results.columns), 6)
            self.assertEqual(results.columns[5].name, "test-props.person-prop-4")

    def test_extracts_descriptions(self) -> None:
        with patch.object(JSONSchemaExtractor, "_get_schema") as mock_get_schema:
            mock_get_schema.return_value = {
                "type": "object",
                "title": "test-schema",
                "properties": {
                    "name": {"type": "string", "description": "name-description"},
                    "age": {"type": "integer"},
                    "test-props": {
                        "type": "object",
                        "properties": {
                            "person-prop-1": {"type": "string"},
                            "person-prop-2": {
                                "type": "integer",
                                "description": "p2-description",
                            },
                        },
                    },
                },
            }

            extractor = JSONSchemaExtractor("test-connection", self.TEST_DATABASE)
            extractor.init(
                self.get_conf(
                    {
                        "object_expand": ["test-props"],
                        "extract_descriptions": True,
                    }
                )
            )

            results = extractor.extract()
            self.assertEqual(results.columns[0].description, "name-description")
            self.assertIsNone(results.columns[1].description)
            self.assertIsNone(results.columns[2].description)
            self.assertIsNone(results.columns[3].description)
            self.assertEqual(results.columns[4].description, "p2-description")

    def test_filters_expanded_columns(self) -> None:
        with patch.object(JSONSchemaExtractor, "_get_schema") as mock_get_schema:
            mock_get_schema.return_value = {
                "type": "object",
                "title": "test-schema",
                "properties": {
                    "name": {"type": "string", "description": "name-description"},
                    "age": {"type": "integer"},
                    "test-props": {
                        "type": "object",
                        "properties": {
                            "person-prop-1": {"type": "string"},
                            "person-prop-2": {
                                "type": "integer",
                                "description": "p2-description",
                            },
                        },
                    },
                },
            }

            extractor = JSONSchemaExtractor("test-connection", self.TEST_DATABASE)
            extractor.init(
                self.get_conf(
                    {"object_expand": ["test-props"], "filter_columns": "^test-props"}
                )
            )

            results = extractor.extract()
            self.assertEqual(len(results.columns), 3)
            self.assertEqual(results.columns[0].name, "test-props")
            self.assertEqual(results.columns[1].name, "test-props.person-prop-1")
            self.assertEqual(results.columns[2].name, "test-props.person-prop-2")

    def test_adds_tags_from_schema(self) -> None:
        with patch.object(JSONSchemaExtractor, "_get_schema") as mock_get_schema:
            mock_get_schema.return_value = {
                "type": "object",
                "title": "test-schema",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                    "glasses": {"type": "boolean"},
                },
                "required": ["name"],
                "oneOf": [
                    {
                        "properties": {
                            "glasses": {"const": True},
                            "focal_length": {"type": "float"},
                        },
                        "required": ["focal_length"],
                        "metadata": {
                            "metadata-key-1": "tag1",
                            "metadata-key-2": "tag2",
                        },
                    },
                    {
                        "properties": {
                            "glasses": {"const": False},
                        },
                    },
                ],
            }

            extractor = JSONSchemaExtractor("test-connection", self.TEST_DATABASE)
            extractor.init(
                self.get_conf({"pivot_column": "glasses", "tags_key": "metadata"})
            )

            results = extractor.extract()
            self.assertEqual(results.name, "True")
            self.assertEqual(len(results.tags), 2)
            self.assertEqual(
                results.tags[0].__repr__(),
                TableTag("metadata-key-1", "tag1").__repr__(),
            )
            self.assertEqual(
                results.tags[1].__repr__(),
                TableTag("metadata-key-2", "tag2").__repr__(),
            )

            results = extractor.extract()
            self.assertEqual(len(results.tags), 0)

            results = extractor.extract()
            self.assertEqual(results, None)
