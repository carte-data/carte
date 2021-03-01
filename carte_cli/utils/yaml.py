from typing import Union, Any, Dict, List
from ruamel.yaml import YAML

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)

def read_file(path: str):
    with open(path, "r") as f:
        data = f.read()

    return yaml.load(data)
