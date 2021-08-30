#!/usr/bin/env python

from typing import Dict, Tuple, Generator
import itertools as it
from pathlib import Path
import tempfile

import click
import numpy as np
import pandas as pd
from tqdm import tqdm
from sklearn.metrics import classification_report
from flyingsquid.label_model import LabelModel
import paranames.io.wikidata_helpers as wh
import paranames.analysis.script_analysis as sa
from p_tqdm import p_map
import orjson

vote_aggregation_methods = set(["all", "any", "majority_vote", "none"])


def apply_entity_disambiguation_rules(
    data: pd.DataFrame,
    id_column: str = "wikidata_id",
    type_column: str = "type",
) -> pd.DataFrame:

    # count how many types for each id
    id_to_ntypes_df = (
        data[[id_column, type_column]]
        .drop_duplicates()
        .groupby(id_column)
        .type.size()
        .reset_index()
        .rename(columns={type_column: "n_types"})
    )

    # join this to the original data frame
    old_nrows = data.shape[0]
    data = data.merge(id_to_ntypes_df, on=id_column)

    # if id is in this dict, it will have several types
    id_to_types = (
        data[data.n_types > 1][[id_column, type_column]]
        .drop_duplicates()
        .groupby(id_column)
        .apply(lambda df: "-".join(sorted(df.type.unique())))
    )

    # if there are no duplicates, get rid of n_types column and return
    if id_to_types.empty:
        data = data.drop(columns="n_types")
        return data
    else:
        id_to_types = id_to_types.to_dict()

    # encode actual disambiguation rules
    entity_disambiguation_rules = {
        "LOC-ORG": "LOC",
        "LOC-ORG-PER": "ORG",
        "ORG-PER": "ORG",
        "LOC-PER": "PER",
    }

    # compose the above two relations
    id_to_canonical_type = {
        _id: entity_disambiguation_rules.get(type_str)
        for _id, type_str in id_to_types.items()
    }

    # replace with canonical types, non-ambiguous ones get None
    canonical_types = data[id_column].apply(
        lambda _id: id_to_canonical_type.get(_id, None)
    )

    # put the old non-ambiguous types back in
    new_types = [
        old_type if new_type is None else new_type
        for old_type, new_type in zip(data.type, canonical_types)
    ]

    data[type_column] = new_types

    # finally drop the extra column we created
    data = data.drop(columns="n_types")

    # also drop duplicate rows
    data = data.drop_duplicates()

    # final check to make sure no id has more than 1 type
    assert all(
        data[[id_column, type_column]]
        .drop_duplicates()
        .groupby(id_column)
        .type.size()
        == 1
    )

    # print out some information to the user
    print("Deduplication complete")
    print(f"No. of rows, original: {old_nrows}")
    print(f"No. of rows, filtered: {data.shape[0]}")
    print(f"Rows removed = {old_nrows - data.shape[0]}")

    return data


def filter_am_ti(
    data: pd.DataFrame,
    id_column: str = "wikidata_id",
    type_column: str = "type",
) -> pd.DataFrame:
    with open("./data/am_ti_kept_ids.txt", encoding="utf8") as f:
        am_ti_kept_ids = set([l.strip() for l in f])

        print(
            f"Loaded {len(am_ti_kept_ids)} IDs to keep for Amharic/Tigrinya..."
        )

    # construct a mask for items that are to be excluded

    def should_keep(row):
        if row.language not in ["am", "ti"]:
            return True
        else:
            id_is_suitable = row[id_column] in am_ti_kept_ids
            try:
                alias_not_latin = bool(not row.is_latin)
            except AttributeError:
                alias_not_latin = True  # if is_latin not found

            return id_is_suitable and alias_not_latin

    keep_these = data.apply(should_keep, axis=1)

    filtered = data[keep_these]

    print("Amharic/Tigrinya filtering complete")
    print(f"No. of rows, original: {data.shape[0]}")
    print(f"No. of rows, filtered: {filtered.shape[0]}")
    print(
        f"No of. am/ti rows in filtered: {filtered[filtered.language.isin(['am', 'ti'])].shape[0]}"
    )
    print(f"Rows removed = {data.shape[0] - filtered.shape[0]}")

    return filtered


@click.command()
@click.option("--input-file", "-i")
@click.option("--output-file", "-o")
@click.option("--io-format", "-f", default="tsv")
@click.option("--id-column", "-id", default="wikidata_id")
@click.option("--type-column", "-t", default="type")
@click.option("--alias-column", "-a", default="alias")
@click.option("--english-column", "-e", default="name")
@click.option("--language-column", "-l", default="language")
def main(
    input_file,
    output_file,
    io_format,
    id_column,
    type_column,
    alias_column,
    english_column,
    language_column,
):

    # read in data
    data = wh.read(input_file, io_format=io_format)

    # drop rows that are not entities (e.g. P-ids)
    data = data[data[id_column].str.startswith("Q")]

    # change <english_column> to "eng"
    data = data.rename(columns={english_column: "eng"})

    # filter rows using entity disambiguation rules
    data = apply_entity_disambiguation_rules(
        data, id_column=id_column, type_column=type_column
    )

    # write to disk
    wh.write(data, output_file, io_format=io_format)


if __name__ == "__main__":
    main()
