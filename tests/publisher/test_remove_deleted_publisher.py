from carte_cli.publisher.remove_deleted_publisher import RemoveDeletedPublisher


def test_get_datasets_to_delete():
    publisher = RemoveDeletedPublisher()
    tables_path = "test-tables-path"

    publisher.tables_path = tables_path

    test_datasets = set(
        [
            "db1/dataset1",
            "db1/dataset3",
            "db1/dataset4",
            "db1/dataset5",
            "db2/dataset1",
        ]
    )

    test_file_paths = [
        f"{tables_path}/db1/dataset1.md",
        f"{tables_path}/db2/dataset1.md",
        f"{tables_path}/db2/dataset2.md",
        f"{tables_path}/db1/dataset3.md",
        f"{tables_path}/db1/dataset4.md",
        f"{tables_path}/db3/dataset1.md",
    ]

    to_delete = [
        dataset
        for dataset in publisher._get_datasets_to_delete(test_datasets, test_file_paths)
    ]

    assert len(to_delete) == 2
    assert "test-tables-path/db3/dataset1.md" in to_delete
    assert "test-tables-path/db2/dataset2.md" in to_delete


def test_output_path_with_trailing_slash():
    publisher = RemoveDeletedPublisher()
    tables_path = "test-tables-path/"

    publisher.tables_path = tables_path

    test_datasets = set(["db1/dataset1", "db2/dataset1"])

    test_file_paths = [
        f"{tables_path}db1/dataset1.md",
        f"{tables_path}db2/dataset1.md",
        f"{tables_path}db2/dataset2.md",
    ]

    to_delete = [
        dataset
        for dataset in publisher._get_datasets_to_delete(test_datasets, test_file_paths)
    ]

    assert len(to_delete) == 1
    assert to_delete[0] == "test-tables-path/db2/dataset2.md"
