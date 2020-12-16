#!/usr/bin/env python3

import argparse
from databuilder.extractor.csv_extractor import CsvExtractor
from databuilder.extractor.glue_extractor import GlueExtractor
from databuilder.job.job import DefaultJob
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer
from pyhocon import ConfigFactory

from flyover.loader.carte_loader import CarteLoader

from flyover.utils.config_parser import parse_config

parser = argparse.ArgumentParser(description="Run metadata extraction for Carte")
parser.add_argument(
    "--config",
    "-c",
    required=True,
    help="The YAML config file that describes connections",
)
parser.add_argument(
    "--output",
    "-o",
    default="data/datasets",
    help="The output directory to place markdown files in.",
)


def run_csv_job(config_file, output_dir):
    carte_loader = CarteLoader()
    extractors = parse_config(config_file)

    job_config = ConfigFactory.from_dict(
        {
            "loader.carte.tables_output_path": output_dir,
        }
    )

    for extractor in extractors:
        task = DefaultTask(extractor=extractor, loader=carte_loader)

        DefaultJob(conf=job_config, task=task).launch()


def main():
    args = parser.parse_args()
    run_csv_job(args.config, args.output)


if __name__ == "__main__":
    main()
