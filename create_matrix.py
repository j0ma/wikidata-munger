import sys
import os
import math
import csv
from typing import IO, Generator, List, Dict, Any, Union, Iterable

import pandas as pd
import numpy as np
import click


def read(input_file: str, io_format: str) -> pd.DataFrame:
    if io_format == "csv":
        return pd.read_csv(input_file, encoding="utf-8")
    else:
        return pd.read_json(input_file, "records", encoding="utf-8")


def write(
    data: pd.DataFrame, output_file: str, io_format: str, index: bool = False
) -> None:
    if io_format == "csv":
        return data.to_csv(output_file, encoding="utf-8", index=index)
    else:
        return data.to_json(output_file, "records", encoding="utf-8", index=index)


def tall_to_wide(df):
    return df.pivot(
        index=["id", "type", "eng"], values="alias", columns="language"
    ).fillna("")


def clean(data):

    # rename column "name" to "eng"
    data = data.rename(columns={"name": "eng"}).copy()

    # remove all names that aren't entities
    data = data[data.id.str.startswith("Q")]

    # remove all tigrinya and amharic that equals english
    ti_or_am = (data.language == "ti") | (data.language == "am")
    alias_equals_eng = data.alias == data.eng
    mask = (ti_or_am) & (alias_equals_eng)
    data = data[~mask]

    return data


@click.command()
@click.option("--input-file", "-i", required=True)
@click.option("--output-file", "-o", required=True)
@click.option("--io-format", "-f", type=click.Choice(["csv", "jsonl"]), default="csv")
def main(input_file, output_file, io_format):

    data = read(input_file, io_format)
    data = clean(data)
    matrix = tall_to_wide(data)
    write(matrix, output_file, io_format, index=(io_format == "csv"))


if __name__ == "__main__":
    main()
