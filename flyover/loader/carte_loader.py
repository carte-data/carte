#!/usr/bin/env python3

import os.path
from pathlib import Path
from typing import Union
from databuilder.loader.base_loader import Loader
from databuilder.models.table_metadata import TableMetadata as DatabuilderTableMetadata
from pyhocon import ConfigTree


from flyover.model.job_metadata import JobMetadata
from flyover.model.carte_table_model import TableMetadata
import flyover.utils.frontmatter as frontmatter

OUTPUT_FILE_PATH = "content/tables"
FRONTMATTER_SEPARATOR = "---"


class CarteLoader(Loader):
    def init(self, conf: ConfigTree):
        self.conf = conf
        self.base_directory = self.conf.get_string("base_directory", ".")
        self.file_path = self.conf.get_string("output_file_path", OUTPUT_FILE_PATH)

    def load(self, record: Union[None, JobMetadata, DatabuilderTableMetadata]):
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
            frontmatter_metadata = TableMetadata.from_frontmatter(metadata, content)
            extractor_metadata = extractor_metadata.merge_with_existing(
                frontmatter_metadata
            )

        frontmatter.dump(full_file_name, *extractor_metadata.to_frontmatter())

    def get_table_file_name(self, record: TableMetadata):
        return f"{self.base_directory}/{self.file_path}/{record.get_file_name()}.md"

    def load_job(self, record: JobMetadata):
        pass

    def get_scope(self) -> str:
        return "carte.loader"
