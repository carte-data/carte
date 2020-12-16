import io
from ruamel.yaml import YAML

yaml = YAML()

FRONTMATTER_SEPARATOR = "---\n"


def parse(filename):
    data = _read_file(filename)

    metadata = {}
    content = ""

    if len(data) >= 1 and data[0] != "":
        metadata = yaml.load(data[0])

    if len(data) >= 2:
        content = data[1]

    return metadata, content

def _read_file(filename: str):
    with open(filename, "r") as f:
        data = f.read().split(FRONTMATTER_SEPARATOR)
    return data

def dump(filename, metadata, content):
    buf = io.StringIO()

    buf.write(FRONTMATTER_SEPARATOR)
    yaml.dump(metadata, buf)
    buf.write(FRONTMATTER_SEPARATOR)
    if content is not None:
        buf.write(content)

    with open(filename, "w") as f:
        print(buf.getvalue(), file=f)
