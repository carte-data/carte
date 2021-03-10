import io
from ruamel.yaml import YAML

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)

FRONTMATTER_SEPARATOR = "---\n"


def parse(filename):
    data = _read_file(filename)

    metadata = {}
    content = ""

    if len(data) >= 2 and data[1] != "":
        metadata = yaml.load(data[1])

    if len(data) >= 3:
        content = data[2]

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
        buf.write(content.strip())

    with open(filename, "w") as f:
        print(buf.getvalue(), file=f)
