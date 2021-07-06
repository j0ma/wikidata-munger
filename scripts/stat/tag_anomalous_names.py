from typing import Dict
import functools as ft

import pandas as pd
import click
import orjson

import script_analysis as sa

from unicodeblock import blocks
import dictances as dt

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

distance_measure_choices = [
    "kullback_leibler",
    "jensen_shannon",
    "bhattacharyya",
]


def mark_as_anomalous(row: pd.Series, critical_values: Dict[str, float]):
    return int(row.distance >= critical_values.get(row.language))


def compute_prototype(
    df: pd.DataFrame, ua: sa.UnicodeAnalyzer
) -> Dict[str, float]:
    return ua.unicode_block_histogram(word=df.alias.str.cat())


def distance(
    p: Dict[str, float],
    q: Dict[str, float],
    distance_measure: str = "kullback_leibler",
) -> float:
    """Computes distance between PMFs p and q using dictances library"""

    return {
        "jensen_shannon": dt.jensen_shannon,
        "kullback_leibler": dt.kullback_leibler,
    }.get(distance_measure)(p, q)


@click.command()
@click.option("--input-file", "-i", required=True)
@click.option("--output-file", "-o", required=True)
@click.option("--io-format", "-f", default="tsv")
@click.option(
    "--distance-measure",
    "-d",
    default="kullback_leibler",
    type=click.Choice(distance_measure_choices),
)
@click.option("--quantile", "-q", default=0.95)
@click.option("--strip", is_flag=True)
@click.option("--no-normalize", is_flag=True)
@click.option("--ignore-punctuation", is_flag=True)
def main(
    input_file: str,
    output_file: str,
    io_format: str,
    distance_measure: str,
    quantile: float,
    strip: bool,
    no_normalize: bool,
    ignore_punctuation: bool,
) -> None:

    # only csv/tsv supported for now
    assert io_format in ["csv", "tsv"]

    # unicode analyzer
    ua = sa.UnicodeAnalyzer(
        strip=strip,
        normalize_histogram=not no_normalize,
        ignore_punctuation=ignore_punctuation,
    )

    data = pd.read_csv(input_file, sep="\t" if io_format == "tsv" else ",")

    # TODO: remove me
    # data = data[~data.eng.str.startswith("Q")]

    prototypes = (
        data.groupby("language")
        .apply(lambda l: compute_prototype(l, ua))
        .to_dict()
    )

    data["distance"] = data.apply(
        lambda row: distance(
            p=ua.unicode_block_histogram(word=row.alias),
            q=prototypes.get(row.language, {}),
            distance_measure=distance_measure,
        ),
        axis=1,
    )

    print("Basic descriptive stats:")
    print(data.groupby("language").distance.describe())

    print(f"{int(100*quantile)}-th percentile:")
    quantiles = data.groupby("language").distance.quantile(q=quantile)
    print(quantiles)

    data["anomalous"] = data.apply(
        lambda row: mark_as_anomalous(row, quantiles), axis=1
    )

    print("What fraction were anomalous?")
    print(data.groupby("language").anomalous.describe())

    print("\nExamples:\n")

    for lang in data.language.unique():
        print(f"Language: {lang}")
        subset = data[data.language == lang]
        anomalous = subset[subset.anomalous == 1]
        non_anomalous = subset[subset.anomalous == 0]

        print("Anomalous:")
        print(anomalous.sample(10))
        print()

        print("Non-anomalous:")
        print(non_anomalous.sample(10))
        print()


if __name__ == "__main__":
    main()
