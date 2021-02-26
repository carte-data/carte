import io
import importlib
from ruamel.yaml import YAML
from carte_cli.extractor.glue_extractor import GlueExtractor

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


CONNECTION_FACTORIES = {
    "glue": create_glue_connection,
    "postgresql": create_postgres_connection,
}


def parse_config(filename):
    data = _read_file(filename)

    parsed_data = yaml.load(data)

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
