import os
import glob
from typing import Tuple
from jinja2 import Environment, PackageLoader, select_autoescape, Template
from pathlib import Path

import carte_cli.utils.frontmatter as frontmatter
from carte_cli.model.carte_table_model import TableMetadata

def _get_flattened_template():
    env = Environment(
        loader=PackageLoader("carte_cli.utils", "templates"),
        autoescape=select_autoescape(["md"]),
    )
    template = env.get_template("dataset_flattened.md")
    return template


def flatten(input_dir: str, output_dir: str) -> None:

    template = _get_flattened_template()

    file_paths = glob.glob(input_dir + "/*/*/*.md", recursive=True)
    print(file_paths)
    output_paths = [
        os.path.join(output_dir, file_path[len(input_dir) :])
        for file_path in file_paths
    ]

    for file_path, output_path in zip(file_paths, output_paths):
        print(f"Flattening dataset {file_path}")
        metadata, content = frontmatter.parse(file_path)
        dataset = TableMetadata.from_frontmatter(metadata, content)
        flattened_metadata, flattened_content = flatten_dataset(dataset, template)
        Path("/".join(output_path.split("/")[:-1])).mkdir(
            parents=True, exist_ok=True
        )
        frontmatter.dump(output_path, flattened_metadata, flattened_content)

    print("Done!")


def flatten_dataset(dataset: TableMetadata, template: Template) -> Tuple[dict, str]:
    metadata = {"title": dataset.name}
    content = template.render(dataset=dataset)
    return metadata, content
