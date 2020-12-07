#!/usr/bin/env python3

from databuilder.extractor.csv_extractor import CsvExtractor
from databuilder.extractor.glue_extractor import GlueExtractor
from databuilder.job.job import DefaultJob
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer
from extractor.extractor.glue_extractor import GlueExtractor as CarteGlueExtractor
from pyhocon import ConfigFactory

from extractor.loader.carte_loader import CarteLoader

def run_csv_job():
    # csv_extractor = CsvExtractor()
    # glue_extractor = GlueExtractor()
    glue_extractor = CarteGlueExtractor()
    carte_loader = CarteLoader()


    task = DefaultTask(
        extractor=glue_extractor, loader=carte_loader, transformer=NoopTransformer()
    )

    job_config = ConfigFactory.from_dict(
        {
            # 'loader.carte.base_directory': '.',
            # 'loader.carte.output_file_path': 'content/tables',
        }
    )

    DefaultJob(conf=job_config, task=task).launch()


if __name__ == "__main__":
    run_csv_job()
