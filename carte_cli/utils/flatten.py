import os
import glob
from typing import Tuple
from jinja2 import Environment, PackageLoader, select_autoescape, Template
from pathlib import Path

from jinja2.loaders import FileSystemLoader

import carte_cli.utils.frontmatter as frontmatter
from carte_cli.model.carte_table_model import TableMetadata


def _get_flattened_template(template_path: str):
    if template_path is None:
        env = Environment(
            loader=PackageLoader("carte_cli.utils", "templates"),
            autoescape=select_autoescape(["md"]),
        )
        template = env.get_template("dataset_flattened.md")

    else:
        env = Environment(loader=FileSystemLoader(searchpath="./"))
        template = env.get_template(template_path)

    return template


def flatten(input_dir: str, output_dir: str, template_path: str) -> None:

    template = _get_flattened_template(template_path)

    file_paths = glob.glob(input_dir + "/*/*/*.md", recursive=True)
    output_paths = [
        os.path.join(output_dir, file_path[(len(input_dir)+1) :])
        for file_path in file_paths
    ]

    for file_path, output_path in zip(file_paths, output_paths):
        metadata, content = frontmatter.parse(file_path)
        dataset = TableMetadata.from_frontmatter(metadata, content)
        flattened_metadata, flattened_content = flatten_dataset(dataset, template)
        Path("/".join(output_path.split("/")[:-1])).mkdir(parents=True, exist_ok=True)
        frontmatter.dump(output_path, flattened_metadata, flattened_content)

    print("Done!")


def flatten_dataset(dataset: TableMetadata, template: Template) -> Tuple[dict, str]:
    metadata = {"title": dataset.name}
    content = template.render(dataset=dataset)
    return metadata, content
