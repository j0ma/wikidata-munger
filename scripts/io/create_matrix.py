import sys
import os
import math
import csv
from typing import IO, Generator, List, Dict, Any, Union, Iterable
from collections import defaultdict, OrderedDict

import wikidata_helpers as wh
import pandas as pd
import numpy as np
import click

from multiprocessing import Pool


def output_matrix(matrix_dict, delimiter, languages, f):
    field_names = ["id", "eng", "type"] + [l for l in languages]
    writer = csv.DictWriter(
        f, delimiter=delimiter, fieldnames=field_names, extrasaction="ignore"
    )

    writer.writeheader()

    def rows(matrix_dict):
        for (wikidata_id, conll_type, eng), d in matrix_dict.items():
            row = {
                "id": wikidata_id,
                "eng": eng,
                "type": conll_type,
            }

            ## we'll use sorted(languages) to make sure order is preserved
            row.update({l: d.get(l, "") for l in sorted(languages)})
            yield row

    writer.writerows(rows(matrix_dict))


def clean(data):

    # data = data.rename(columns={"name": "eng"})

    # remove all names that aren't entities
    data = data[data.id.str.startswith("Q")]

    return data


def munge(d_in):
    out = []

    for (_id, _cat, _eng, _lang), _alias in d_in.items():
        out.append({(_id, _cat, _eng): {_lang: _alias}})

    return out


def process_chunk(data):

    unique_langs = set()
    matrix_dict = defaultdict(dict)
    data = clean(data)
    chunk_dict = data.set_index(
        ["id", "type", "eng", "language"]
    ).alias.to_dict()

    for entity_alias_dict in munge(chunk_dict):
        for entity_record, aliases in entity_alias_dict.items():
            unique_langs = unique_langs.union(aliases)
            matrix_dict[entity_record].update(aliases)

    return matrix_dict, unique_langs


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
@click.option("--n-jobs", "-n", type=int, default=10)
def main(input_file, output_file, io_format, chunksize, n_jobs):

    data_chunks = wh.read(input_file, io_format, chunksize=chunksize)
    matrix_dict = defaultdict(dict)

    rows_processed = 0

    with Pool(n_jobs) as pool:
        print("Computing all the disjoint matrix dicts")
        matrix_dicts = pool.map(func=process_chunk, iterable=data_chunks)

    print("Done! Now joining them to one big dict")

    unique_langs = set()

    for md, ul in matrix_dicts:
        unique_langs = unique_langs.union(ul)
        matrix_dict.update(md)

    # convert to OrderedDict to preserve order
    matrix_dict = OrderedDict(matrix_dict)

    print(f"Done! Now writing to disk under {output_file}")
    with open(output_file, "w") as tsv_out:
        output_matrix(
            matrix_dict, delimiter="\t", languages=unique_langs, f=tsv_out
        )


if __name__ == "__main__":
    main()
