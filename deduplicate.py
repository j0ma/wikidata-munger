#!/usr/bin/env python

# deduplicate.py

import click
import pandas as pd
from wikidata_helpers import LatinChecker


def read(input_file: str, io_format: str) -> pd.DataFrame:
    if io_format == "csv":
        return pd.read_csv(input_file, encoding="utf-8")
    else:
        return pd.read_json(input_file, "records", encoding="utf-8")


def write(data: pd.DataFrame, output_file: str, io_format: str) -> None:
    if io_format == "csv":
        return data.to_csv(output_file, encoding="utf-8", index=False)
    else:
        return data.to_json(
            output_file, "records", encoding="utf-8", index=False
        )


def deduplicate(data: pd.DataFrame) -> pd.DataFrame:

    # count how many types for each id
    id_to_ntypes_df = (
        data[["id", "type"]]
        .drop_duplicates()
        .groupby("id")
        .type.size()
        .reset_index()
        .rename(columns={"type": "n_types"})
    )

    # join this to the original data frame
    data_old = data.copy()  # copy this just in case
    data = data.merge(id_to_ntypes_df, on="id")

    # if id is in this dict, it will have several types
    id_to_type_string = (
        data[data.n_types > 1][["id", "type"]]
        .drop_duplicates()
        .groupby("id")
        .apply(lambda df: "-".join(sorted(df.type.unique())))
        .to_dict()
    )

    # encode actual trumping rules
    trumping_rules = {
        "LOC-ORG": "LOC",  # countries
        "LOC-ORG-PER": "ORG",  # native american tribes
        "ORG-PER": "ORG",  # these are mostly manufacturers
        "LOC-PER": "PER",  # jj thomson, Q47285
    }

    # compose the above two relations
    id_to_canonical_type = {
        _id: trumping_rules.get(type_str)

        for _id, type_str in id_to_type_string.items()
    }

    # replace with canonical types, non-ambiguous ones get None
    canonical_types = data.id.apply(
        lambda _id: id_to_canonical_type.get(_id, None)
    )

    # put the old non-ambiguous types back in
    new_types = [
        old_type if new_type is None else new_type

        for old_type, new_type in zip(data.type, canonical_types)
    ]

    data["type"] = new_types

    # finally drop the extra column we created
    data = data.drop("n_types", 1)

    # also drop duplicate rows
    data = data.drop_duplicates()

    # final check to make sure no id has more than 1 type
    assert all(
        data[["id", "type"]].drop_duplicates().groupby("id").type.size() == 1
    )

    # print out some information to the user
    print("Deduplication complete")
    print(f"No. of rows, original: {data_old.shape[0]}")
    print(f"No. of rows, dedupilcated: {data.shape[0]}")
    print(f"Rows removed = {data_old.shape[0] - data.shape[0]}")

    return data


@click.command()
@click.option("--input-csv", "-i")
@click.option("--output-csv", "-o")
@click.option("--alias-column", "-a", default="alias")
@click.option("--english-column", "-e", default="name")
def main(input_csv, output_csv, alias_column, english_column):

    latin_checker = LatinChecker()

    # read in data
    data = read(input_csv, io_format="csv")

    # change <english_column> to "english"
    data = data.rename(columns={english_column: "eng"})

    # add is_latin column
    data["is_latin"] = data[alias_column].apply(latin_checker)

    # deduplicate rows using trumping rules
    data = deduplicate(data)

    # write to disk
    write(data, output_csv, io_format="csv")


if __name__ == "__main__":
    main()
