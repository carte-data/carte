from typing import Union, Any, Dict, List
from ruamel.yaml import YAML
import json

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)


def read_yaml(path: str):
    with open(path, "r") as f:
        data = f.read()

    return yaml.load(data)

def write_yaml(data, path: str, **kwargs):
    with open(path, "w") as f:
        yaml.dump(data, f, **kwargs)


def read_json(path: str):
    with open(path, "r") as f:
        data = json.load(f)

    return data


def write_json(data, path: str, **kwargs):
    with open(path, "w") as f:
        json.dump(data, f, **kwargs)
