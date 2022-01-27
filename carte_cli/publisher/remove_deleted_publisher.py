import os
import glob
from typing import Iterator, List, Set
from databuilder.publisher.base_publisher import Publisher
from pyhocon.config_tree import ConfigTree


class RemoveDeletedPublisher(Publisher):
    def init(self, conf: ConfigTree) -> None:
        self.conf = conf
        self.manifests_path = self.conf.get_string("manifests_path")
        self.tables_path = self.conf.get_string("tables_output_path")

        if self.tables_path is None:
            raise ValueError("Output path is needed for publisher")

    def _get_datasets_to_delete(
        self, datasets: Set[str], file_paths: List[str]
    ) -> Iterator[str]:
        prefix_length = len(self.tables_path)
        if not self.tables_path.endswith("/"):
            prefix_length += 1

        file_ids = [path[prefix_length : -(len(".md"))] for path in file_paths]

        for file_path, file_id in zip(file_paths, file_ids):
            if file_id not in datasets:
                yield file_path

    def publish_impl(self) -> None:
        print("Publishing")
        print(f"Tables path: {self.tables_path}")
        with open(self.manifests_path) as f:
            lines = f.readlines()
        datasets = set([line.strip() for line in lines])

        file_paths = glob.glob(self.tables_path + "/*/*/*.md", recursive=True)

        for file_path in self._get_datasets_to_delete(datasets, file_paths):
            os.remove(file_path)
            print(f"Removed {file_path}")

    def get_scope(self) -> str:
        return "publisher.carte"
