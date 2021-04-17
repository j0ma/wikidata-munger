import sys
import os
import math
import csv
from typing import IO, Generator, List, Dict, Any, Union, Iterable
from collections import defaultdict

import wikidata_helpers as wh
import pandas as pd
import numpy as np
import click
from tqdm import tqdm


def tall_to_wide(df):

    return df.pivot(
        index=["id", "type", "eng"], values="alias", columns="language"
    ).fillna("")


def output_matrix(matrix_dict, delimiter, languages, f):
    field_names = ["wikidata_id", "eng", "type"] + languages
    writer = csv.DictWriter(f, delimiter=delimiter, fieldnames=field_names)

    writer.writeheader()

    def rows(matrix_dict):
        for (wikidata_id, conll_type, eng), d in matrix_dict.items():
            row = {
                "wikidata_id": wikidata_id,
                "eng": eng,
                "type": conll_type,
            }
            row.update({l: d.get(l, "") for l in languages})
            yield row

    writer.writerows(rows(matrix_dict))


def convert_to_matrix(matrix_dict):
    matrix = pd.DataFrame.from_dict(matrix_dict, orient="index")
    matrix.index.rename(["wikidata_id", "type", "eng"], inplace=True)

    return matrix.fillna("")  # .reset_index()


def clean(data):

    data = data.rename(columns={"name": "eng", "id": "wikidata_id"})

    # remove all names that aren't entities
    data = data[data.wikidata_id.str.startswith("Q")]

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
@click.option("--chunksize", "-c", type=int, default=1000)
def main(input_file, output_file, io_format, chunksize):

    # data_chunks = wh.read(input_file, io_format, chunksize=chunksize)
    # matrix_dict = defaultdict(dict)

    # def munge(d_in):
    # out = []

    # for (_id, _cat, _eng, _lang), _alias in d_in.items():
    # out.append({(_id, _cat, _eng): {_lang: _alias}})

    # return out

    # rows_processed = 0

    # unique_langs = set()

    # for chunk_ix, data in tqdm(enumerate(data_chunks, start=1)):
    # print(f"Processing chunk #{chunk_ix}...")
    # data = clean(data)
    # chunk_dict = data.set_index(
    # ["id", "type", "eng", "language"]
    # ).alias.to_dict()

    # for entity_alias_dict in munge(chunk_dict):
    # for entity_record, aliases in entity_alias_dict.items():
    # unique_langs = unique_langs.union(set(list(aliases.keys())))
    # matrix_dict[entity_record].update(aliases)
    # rows_processed += data.shape[0]
    # print(f"Done! Total rows processed: {rows_processed:,}\n")

    import pickle

    with open("/tmp/wikidata_matrix_dict.pkl", "rb") as f:
        matrix_dict = pickle.load(f)

    with open("/tmp/wikidata_unique_langs.pkl", "rb") as f:
        unique_langs = pickle.load(f)

    with open("/tmp/wikidata_matrix_test.tsv", "w") as tsv_out:
        output_matrix(
            matrix_dict, delimiter="\t", languages=unique_langs, f=tsv_out
        )

    # print("Converting to matrix...")
    # matrix = convert_to_matrix(matrix_dict)

    # print("Saving to disk...")
    # wh.write(
    # matrix, output_file, io_format, index=(io_format in ["csv", "tsv"])
    # )


if __name__ == "__main__":
    main()
