from carte_cli.extractor.json_schema_extractor import JSONSchemaExtractor
import io
import importlib
from pyhocon.exceptions import ConfigException
from ruamel.yaml import YAML
from carte_cli.extractor.glue_extractor import GlueExtractor
from carte_cli.utils.file_io import read_yaml

yaml = YAML()

CONFIG_KEY = "config"


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
    try:
        connection_name = conn_dict.get("name", "json_schema")
        database = config["database"]
        schema_path = config["schema_path"]
    except KeyError:
        raise ConfigException(
            "The name, database, and schema_path values are required for JSON Schema connections"
        )
    return (
        JSONSchemaExtractor(
            connection_name,
            database,
            schema_path,
            pivot_column=config.get("pivot_column"),
            object_expand=config.get("object_expand"),
            extract_descriptions=config.get("extract_descriptions", False),
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
    config = {}

    for conn_dict in connections:
        extractor, extractor_config = CONNECTION_FACTORIES[conn_dict.get("type")](
            conn_dict
        )
        custom_config = {}
        scope = extractor.get_scope()
        config = conn_dict.get(CONFIG_KEY, {})
        for conf_key, conf_value in config.items():
            custom_config[f"{scope}.{conf_key}"] = conf_value

        config = {**config, **extractor_config, **custom_config}
        extractors.append(extractor)

    return extractors, config


def _read_file(filename: str):
    with open(filename, "r") as f:
        config_str = f.read()
    return config_str
