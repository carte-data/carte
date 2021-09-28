from typing import List
from carte_cli.extractor.json_schema_extractor import JSONSchemaExtractor
import io
import importlib
from pyhocon.exceptions import ConfigException
from ruamel.yaml import YAML
from carte_cli.extractor.glue_extractor import GlueExtractor
from carte_cli.utils.file_io import read_yaml

yaml = YAML()

CONFIG_KEY = "config"


def check_required_config_present(required_keys: List[str], config_dict: dict):
    config_keys_set = set(config_dict.keys())
    required_keys_set = set(required_keys)
    diff = required_keys_set - config_keys_set
    return None if len(diff) == 0 else diff


def create_glue_connection(conn_dict):
    return GlueExtractor(conn_dict.get("name", "glue")), {}


def create_postgres_connection(conn_dict):
    PostgresMetadataExtractor = importlib.import_module(
        "databuilder.extractor.postgres_metadata_extractor"
    ).PostgresMetadataExtractor
    extractor = PostgresMetadataExtractor()
    extractor_scope = extractor.get_scope()
    config = conn_dict.get(CONFIG_KEY, {})
    included_schemas = config.get("included_schemas", ["public"])
    schemas_sql_list = "('{schemas}')".format(schemas="', '".join(included_schemas))
    schema_where_clause = f"where table_schema in {schemas_sql_list}"

    connection_string = config.get("connection_string")
    if connection_string is None:
        raise KeyError(
            "connection_string is a required config item for PostgreSQL connections"
        )

    return PostgresMetadataExtractor(), {
        f"{extractor_scope}.{PostgresMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}": schema_where_clause,
        f"{extractor_scope}.extractor.sqlalchemy.conn_string": connection_string,
    }


def create_json_schema_connection(conn_dict):
    config = conn_dict.get(CONFIG_KEY, {})
    connection_name = conn_dict.get("name", "json_schema")
    database = config.get("database")
    missing_params = check_required_config_present(
        JSONSchemaExtractor.required_config_keys(), config
    )
    if missing_params is not None:
        raise ConfigException(
            f"The following values are required for JSON Schema connections: {missing_params}"
        )
    return (
        JSONSchemaExtractor(
            connection_name,
            database,
        ),
        {},
    )


CONNECTION_FACTORIES = {
    "glue": create_glue_connection,
    "postgresql": create_postgres_connection,
    "json_schema": create_json_schema_connection,
}


def parse_config(filename):
    parsed_data = read_yaml(filename)

    connections = parsed_data.get("connections", [])
    extractors = []
    global_config = {}

    for conn_dict in connections:
        extractor, extractor_config = CONNECTION_FACTORIES[conn_dict.get("type")](
            conn_dict
        )
        custom_config = {}
        scope = extractor.get_scope()
        config = conn_dict.get(CONFIG_KEY, {})
        for conf_key, conf_value in config.items():
            custom_config[f"{scope}.{conf_key}"] = conf_value

        global_config = {**global_config, **extractor_config, **custom_config}
        extractors.append(extractor)

    return extractors, global_config


def _read_file(filename: str):
    with open(filename, "r") as f:
        config_str = f.read()
    return config_str
