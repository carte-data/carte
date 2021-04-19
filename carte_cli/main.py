#!/usr/bin/env python3

import os
import typer
import click_spinner
from databuilder.extractor.csv_extractor import CsvExtractor
from databuilder.extractor.glue_extractor import GlueExtractor
from databuilder.job.job import DefaultJob
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer
from pyhocon import ConfigFactory

from carte_cli.loader.carte_loader import CarteLoader, MANIFESTS_FILE
from carte_cli.publisher.remove_deleted_publisher import RemoveDeletedPublisher
from carte_cli.utils.config_parser import parse_config
from carte_cli.scaffolding.frontend import create_frontend_dir
from carte_cli.utils.flatten import flatten as execute_flatten

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
    manifests_file = os.path.join(output_dir, MANIFESTS_FILE)
    if os.path.isfile(manifests_file):
        os.remove(manifests_file)

    carte_loader = CarteLoader()
    remove_deleted_publisher = RemoveDeletedPublisher()
    extractors, config = parse_config(config_path)

    job_config = ConfigFactory.from_dict(
        {
            "loader.carte.tables_output_path": output_dir,
            "loader.carte.manifests_path": manifests_file,
            "publisher.carte.tables_output_path": output_dir,
            "publisher.carte.manifests_path": manifests_file,
            **config,
        }
    )

    typer.echo("Running extraction...")

    with click_spinner.spinner():
        for index, extractor in enumerate(extractors):
            task = DefaultTask(extractor=extractor, loader=carte_loader)
            job_args = dict(conf=job_config, task=task)
            if index == len(extractors) - 1:  # if last job, remove deleted tables
                job_args["publisher"] = remove_deleted_publisher

            DefaultJob(**job_args).launch()

    typer.echo("Done!")


@app.command("new")
def new_frontend(
    name: str = typer.Argument(..., help="The name of the front end folder to create"),
    no_admin: bool = typer.Option(
        False, "--no-admin", help="Disable admin for editing metadata"
    ),
    no_sample: bool = typer.Option(
        False, "--no-sample", help="Don't initialise sample data"
    ),
):
    create_frontend_dir(name, init_admin=(not no_admin), sample_data=(not no_sample))


@app.command("flatten")
def flatten(
    input_dir: str = typer.Argument(
        ..., help="The source metadata directory, the same as the extraction output"
    ),
    output_dir: str = typer.Argument(
        ..., help="The destination directory for flattened markdown files"
    ),
    template: str = typer.Option(
        None, "--template", "-t", help="The template to use for flattening datasets"
    ),
):
    execute_flatten(input_dir, output_dir, template)


if __name__ == "__main__":
    app()
