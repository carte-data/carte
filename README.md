# Carte
[![PyPI version](https://badge.fury.io/py/carte-cli.svg)](https://badge.fury.io/py/carte-cli)
![PyPI - License](https://img.shields.io/pypi/l/carte-cli)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/carte_cli.svg)](https://pypi.org/project/carte-cli/)


Carte is a Python library for extracting metadata from your data sources and generating structured Markdown files from it. It can also scaffold an MkDocs based static site that provides an easy to use, fully searchable UI that you can simply upload to any static site hosting provider. Carte currently supports the AWS Glue catalog, PostgreSQL, and JSON schemas (only basic support). More sources are coming soon. (You can also use sources from the [Amundsen Databuilder](https://github.com/amundsen-io/amundsendatabuilder) library with a bit of scripting, see [the docs](https://docs.cartedata.com/reference/databuilder/) for details)


## Installation

``` sh
pip install carte-cli
```

If you plan to use PostgreSQL as a data source, you should also install the related optional dependencies using the following command instead of the first one:

``` sh
pip install carte-cli[postgres]
```



## Usage

See [the docs](https://docs.cartedata.com)
