import sys
import os
import math
import csv
from typing import IO, Generator, List, Dict, Any, Union, Iterable

import wikidata_helpers as wh
import pandas as pd
import click


def get_output_filename(input_file: str, language: str) -> str:
    prefix, extension = os.path.splitext(input_file)

    return f"{prefix}.{language}{extension}"


@click.command()
@click.option(
    "--lang-column", "-c", default="language", help="Language column"
)
@click.option("--input-file", "-i", required=True)
@click.option(
    "--io-format",
    "-f",
    type=click.Choice(["csv", "jsonl", "tsv"]),
    default="csv",
    help="I/O format",
)
def main(lang_column, input_file, io_format):

    data = wh.read(input_file, io_format)

    for lang in data[lang_column].unique():
        filtered = data[data[lang_column] == lang]

        output_file = get_output_filename(input_file, lang)

        wh.write(filtered, output_file, io_format)


if __name__ == "__main__":
    main()
