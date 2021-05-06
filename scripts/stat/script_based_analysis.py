from collections import Counter

import pandas as pd
import click
import orjson

import wikidata_helpers as wh

from unicodeblock import blocks

import dask.dataframe as dd


@click.command()
@click.option("--input-file", "-i", required=True)
@click.option("--io-format", "-f", default="tsv")
@click.option("--num-workers", "-w", default=12, type=int)
def main(input_file: str, io_format: str, num_workers: int) -> None:

    # only csv/tsv supported for now
    assert io_format in ["csv", "tsv"]

    # data = dd.from_pandas(wh.read(input_file, io_format), npartitions=NPART)
    # TODO: refactor dask use into read() or so
    data = dd.read_csv(input_file, sep="\t" if io_format == "tsv" else ",")

    with open("./data/human_readable_lang_names.json", encoding="utf8") as f:
        human_readable_names = orjson.loads(f.read())

    data["language_long"] = data.language.apply(
        lambda l: human_readable_names.get(l), meta=("language", "str")
    )

    data["script"] = data.alias.apply(
        wh.most_common_unicode_block, meta=("alias", "str")
    )

    most_common_scripts = (
        data.groupby(["language", "type"])
        .script.apply(
            lambda s: s.value_counts().idxmax(), meta=("s", "object")
        )
        .compute()
        .to_dict()
    )

    def keep_if_most_common_script(row):
        return row.script == most_common_scripts.get((row.language, row.type))

    keep_mask = data.apply(keep_if_most_common_script, axis=1)
    filtered = data[keep_mask]

    data = data.compute(num_workers=num_workers)
    filtered = filtered.compute(num_workers=num_workers)

    print("Writing to disk...")
    data.to_csv("/tmp/wikidata_all_with_script.tsv", sep="\t", index=False)
    filtered.to_csv(
        "/tmp/wikidata_scripts_filtered.tsv", sep="\t", index=False
    )

    orig_length = data.shape[0]
    filtered_length = filtered.shape[0]
    print("Done!")
    print(f"Rows, original data: {orig_length:,}")
    print(f"Rows, filtered data: {filtered_length:,}")
    print(f"Difference: {orig_length - filtered_length:,}")


if __name__ == "__main__":
    main()
