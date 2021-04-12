import sys
import os
import math
import csv
from typing import IO, Generator, List, Dict, Any, Union, Iterable

import wikidata_helpers as wh
import pandas as pd
import numpy as np
import click


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
@click.option(
    "--io-format",
    "-f",
    type=click.Choice(["csv", "tsv", "jsonl"]),
    default="tsv",
)
def main(input_file, output_file, io_format):

    data = wh.read(input_file, io_format)
    data = clean(data)
    matrix = tall_to_wide(data)
    wh.write(
        matrix, output_file, io_format, index=(io_format in ["csv", "tsv"])
    )


if __name__ == "__main__":
    main()
