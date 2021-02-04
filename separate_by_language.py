import sys
import os
import math
import csv
from typing import IO, Generator, List, Dict, Any, Union, Iterable

import pandas as pd
import click


def read(input_file: str, io_format: str) -> pd.DataFrame:
    if io_format == "csv":
        return pd.read_csv(input_file, encoding="utf-8")
    else:
        return pd.read_json(input_file, "records", encoding="utf-8")


def write(data: pd.DataFrame, output_file: str, io_format: str) -> None:
    if io_format == "csv":
        return data.to_csv(output_file, encoding="utf-8", index=False)
    else:
        return data.to_json(output_file, "records", encoding="utf-8", index=False)


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
    type=click.Choice(["csv", "jsonl"]),
    default="csv",
    help="I/O format",
)
def main(lang_column, input_file, io_format):

    data = read(input_file, io_format)

    for lang in data[lang_column].unique():
        filtered = data[data[lang_column] == lang]

        output_file = get_output_filename(input_file, lang)

        write(filtered, output_file, io_format)


if __name__ == "__main__":
    main()
