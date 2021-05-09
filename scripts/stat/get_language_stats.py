from collections import Counter

import pandas as pd
import click
import orjson

import unicode_helpers as uh

from unicodeblock import blocks

import dask.dataframe as dd

import numpy as np
import scipy.stats as sps


def alias_script_tuples(f_cache):
    for line in f_cache:
        try:
            alias, script = line.strip().split("\t")
            yield alias, script
        except:
            continue


def load_cache(cache_path):
    with open(cache_path, encoding="utf-8") as f:
        return {k: v for k, v in alias_script_tuples(f)}


def most_common_script(scripts):
    return Counter(scripts).most_common(1)[0][0]


def compute_entropy(series):
    return sps.entropy(series.value_counts())


@click.command()
@click.option("--input-file", "-i", required=True)
@click.option(
    "--cache-path",
    required=True,
    help="Path to cached alias -> script mappings",
)
@click.option("--io-format", "-f", default="tsv")
@click.option("--num-workers", "-w", default=12, type=int)
def main(
    input_file: str,
    cache_path: str,
    io_format: str,
    num_workers: int,
) -> None:

    # only csv/tsv supported for now
    assert io_format in ["csv", "tsv"]

    # alias_to_script = load_cache(cache_path)
    alias_to_script = pd.read_csv(
        cache_path, sep="\t", names="alias,script".split(",")
    )

    with open("./data/human_readable_lang_names.json", encoding="utf8") as f:
        human_readable_names = orjson.loads(f.read())

    data = pd.read_csv(input_file, sep="\t" if io_format == "tsv" else ",")

    data["language_long"] = data.language.apply(
        lambda l: human_readable_names.get(l)
    )

    data = pd.merge(data, alias_to_script, on="alias", how="left")

    script_entropies = dd.from_pandas(
        data.groupby("language")
        .script.apply(compute_entropy)
        .round(3)
        .sort_values(ascending=False)
        .reset_index(),
        chunksize=5000
    )
    script_entropies.compute()

    print(script_entropies)


if __name__ == "__main__":
    main()
