#!/usr/bin/env python

import click
import pandas as pd
import wikidata_helpers as wh


def remove_anomalous(data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    return data


def standardize_names(data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    return data


def apply_trumping_rules(
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
    id_to_type_string = (
        data[data.n_types > 1][[id_column, type_column]]
        .drop_duplicates()
        .groupby(id_column)
        .apply(lambda df: "-".join(sorted(df.type.unique())))
        .to_dict()
    )

    # TODO: update these
    # encode actual trumping rules
    trumping_rules = {
        "LOC-ORG": "LOC",
        "LOC-ORG-PER": "ORG",
        "ORG-PER": "ORG",
        "LOC-PER": "PER",
    }

    # compose the above two relations
    id_to_canonical_type = {
        _id: trumping_rules.get(type_str)

        for _id, type_str in id_to_type_string.items()
    }

    # replace with canonical types, non-ambiguous ones get None
    canonical_types = data.wikidata_id.apply(
        lambda _id: id_to_canonical_type.get(_id, None)
    )

    # put the old non-ambiguous types back in
    new_types = [
        old_type if new_type is None else new_type

        for old_type, new_type in zip(data.type, canonical_types)
    ]

    data[type_column] = new_types

    # finally drop the extra column we created
    data = data.drop("n_types", 1)

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
    print(f"No. of rows, deduplicated: {data.shape[0]}")
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
            id_is_suitable = row.wikidata_id in am_ti_kept_ids
            # alias_not_eng = row.alias != row.eng
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
@click.option("--id-column", "-a", default="wikidata_id")
@click.option("--type-column", "-a", default="type")
@click.option("--alias-column", "-a", default="alias")
@click.option("--english-column", "-e", default="name")
def main(
    input_file,
    output_file,
    io_format,
    id_column,
    type_column,
    alias_column,
    english_column,
):

    # read in data
    data = wh.read(input_file, io_format=io_format)

    # drop rows that are not entities (e.g. P-ids)
    data = data[data.wikidata_id.str.startswith("Q")]

    # change <english_column> to "eng"
    data = data.rename(columns={english_column: "eng"})

    # deduplicate rows using trumping rules
    data = apply_trumping_rules(
        data, id_column=id_column, type_column=type_column
    )

    # remove wrong script / anomalous names
    data = remove_anomalous(data, id_column=id_column, type_column=type_column)

    # standardize names (PER)
    data = standardize_names(
        data, id_column=id_column, type_column=type_column
    )

    # filter amharic & tigrinya
    data = filter_am_ti(data, id_column=id_column, type_column=type_column)

    # write to disk
    wh.write(data, output_file, io_format=io_format)


if __name__ == "__main__":
    main()
