#!/usr/bin/env python3

import typer
import click_spinner
from databuilder.extractor.csv_extractor import CsvExtractor
from databuilder.extractor.glue_extractor import GlueExtractor
from databuilder.job.job import DefaultJob
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer
from pyhocon import ConfigFactory

from carte.loader.carte_loader import CarteLoader

from carte.utils.config_parser import parse_config

app = typer.Typer()


@app.command("extract")
def run_extraction(
    config_path: str = typer.Argument(..., help="The path to the config YAML file"),
    output_dir: str = typer.Option(".", "--output", "-o", help="The output path"),
):
    """
    Extract metadata from data sources and write to files.
    The first argument is the path to the extraction config file.
    Optionally, you can set an --output directory. By default it uses the current
    working directory.
    """
    carte_loader = CarteLoader()
    extractors, config = parse_config(config_path)

    job_config = ConfigFactory.from_dict(
        {"loader.carte.tables_output_path": output_dir, **config}
    )

    print("Running extraction...")

    with click_spinner.spinner():

        for extractor in extractors:
            task = DefaultTask(extractor=extractor, loader=carte_loader)

            DefaultJob(conf=job_config, task=task).launch()

    print("Done!")


if __name__ == "__main__":
    app()
