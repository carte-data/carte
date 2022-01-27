import json


def load_json(filepath):
    with open(filepath, "r") as f:
        test_dict = json.load(f)

    return test_dict
