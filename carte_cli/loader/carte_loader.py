#!/usr/bin/env python3

import os.path
from pathlib import Path
from typing import Union
from databuilder.loader.base_loader import Loader
from databuilder.models.table_metadata import TableMetadata as DatabuilderTableMetadata
from pyhocon import ConfigTree


from carte_cli.model.job_metadata import JobMetadata
from carte_cli.model.carte_table_model import TableMetadata
import carte_cli.utils.frontmatter as frontmatter

TABLES_OUTPUT_PATH = "content/tables"
JOBS_OUTPUT_PATH = "content/jobs"
FRONTMATTER_SEPARATOR = "---"


class CarteLoader(Loader):
    def init(self, conf: ConfigTree):
        self.conf = conf
        self.base_directory = self.conf.get_string("base_directory", ".")
        self.tables_path = self.conf.get_string(
            "tables_output_path", TABLES_OUTPUT_PATH
        )
        self.jobs_path = self.conf.get_string("jobs_output_path", JOBS_OUTPUT_PATH)

    def load(
        self, record: Union[None, JobMetadata, DatabuilderTableMetadata, TableMetadata]
    ):
        if not record:
            return

        record_handlers = {
            JobMetadata: self.load_job,
            DatabuilderTableMetadata: self.load_table,
            TableMetadata: self.load_table,
        }

        if type(record) in record_handlers:
            type_handler = record_handlers[type(record)]
            type_handler(record)

    def load_table(self, record: Union[DatabuilderTableMetadata, TableMetadata]):
        if type(record) == DatabuilderTableMetadata:
            extractor_metadata = TableMetadata.from_databuilder(record)
        else:
            extractor_metadata = record

        full_file_name = self.get_table_file_name(extractor_metadata)
        Path("/".join(full_file_name.split("/")[:-1])).mkdir(
            parents=True, exist_ok=True
        )

        if os.path.isfile(full_file_name):
            metadata, content = frontmatter.parse(full_file_name)
            try:
                frontmatter_metadata = TableMetadata.from_frontmatter(metadata, content)
                extractor_metadata = extractor_metadata.merge_with_existing(
                    frontmatter_metadata
                )
            except ValueError as e:
                raise ValueError(f"{e}\nFile name: {full_file_name}")

        frontmatter.dump(full_file_name, *extractor_metadata.to_frontmatter())

    def get_table_file_name(self, record: TableMetadata):
        return os.path.join(self.base_directory, self.tables_path, f"{record.get_file_name()}.md")

    def load_job(self, record: JobMetadata):
        pass

    def get_scope(self) -> str:
        return "loader.carte"
