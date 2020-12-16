import io
from ruamel.yaml import YAML
from flyover.extractor.glue_extractor import GlueExtractor

yaml = YAML()


def create_glue_connection(conn_dict):
    return GlueExtractor(conn_dict.get("name", "glue"))


CONNECTION_FACTORIES = {"glue": create_glue_connection}


def parse_config(filename):
    data = _read_file(filename)

    parsed_data = yaml.load(data)

    connections = parsed_data.get("connections", [])

    extractors = [
        CONNECTION_FACTORIES[conn_dict.get("type")](conn_dict)
        for conn_dict in connections
    ]

    return extractors


def _read_file(filename: str):
    with open(filename, "r") as f:
        config_str = f.read()
    return config_str
