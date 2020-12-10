import os
import unittest
from unittest.mock import patch, mock_open
import pytest

import flyover.utils.frontmatter as frontmatter


@patch("flyover.utils.frontmatter._read_file", autospec=True)
class TestFrontmatter(unittest.TestCase):
    def test_parse_empty_file(self, mock_read_file):
        mock_read_file.return_value = []

        metadata, content = frontmatter.parse("mock-file")
        mock_read_file.assert_called_once_with("mock-file")
        assert metadata == {}
        assert content == ""

    @patch("flyover.utils.frontmatter.yaml")
    def test_read_only_yaml(self, mock_yaml, mock_read_file):
        mock_read_file.return_value = ["mock-yaml"]

        mock_metadata = {"mock-metadata": "meta"}

        mock_yaml.load.return_value = mock_metadata

        metadata, content = frontmatter.parse("mock-file")

        mock_read_file.assert_called_once_with("mock-file")
        mock_yaml.load.assert_called_once_with("mock-yaml")
        assert metadata == mock_metadata
        assert content == ""

    @patch("flyover.utils.frontmatter.yaml")
    def test_read_with_content(self, mock_yaml, mock_read_file):
        mock_read_file.return_value = ["mock-yaml", "mock-content"]

        mock_metadata = {"mock-metadata": "meta"}

        mock_yaml.load.return_value = mock_metadata

        metadata, content = frontmatter.parse("mock-file")

        mock_read_file.assert_called_once_with("mock-file")
        mock_yaml.load.assert_called_once_with("mock-yaml")
        assert metadata == mock_metadata
        assert content == "mock-content"

    @patch("flyover.utils.frontmatter.print")
    def test_write_prints_correct_content(self, mock_print, mock_read_file):
        m = mock_open()
        with patch("flyover.utils.frontmatter.open", m):

            frontmatter.dump("test-file", {"mock-metadata": "meta"}, "mock-content")

            m.assert_called_once_with("test-file", "w")
            handle = m()

            mock_print.assert_called_with(
                "---\nmock-metadata: meta\n---\nmock-content", file=handle
            )
