import sys
import os
import math
import csv
from typing import IO, Generator, List, Dict, Any, Union, Iterable

from pymongo import MongoClient
import wikidata_helpers as wh
import click


def output_jsonl(
    document: wh.WikidataRecord,
    f: IO,
    languages: Iterable[str],
    strict: bool = False,
    row_number: int = 0,
) -> None:
    wikidata_id = document.id
    language_set = set(languages)

    for lang, alias in document.aliases.items():
        if strict and lang not in language_set:
            continue
        row = wh.orjson_dump(
            {"id": wikidata_id, "alias": alias, "language": lang}
        )
        f.write(f"{row}\n")


def output_csv(
    document: wh.WikidataRecord,
    f: IO,
    languages: Iterable[str],
    strict: bool = False,
    row_number: int = 0,
    delimiter: str = ",",
) -> None:
    language_set = set(languages)
    wikidata_id = document.id
    writer = csv.DictWriter(f, fieldnames=["id", "alias", "language"])

    if row_number == 0:
        writer.writeheader()
    rows = (
        {"id": wikidata_id, "alias": alias, "language": lang}

        for lang, alias in document.aliases.items()
    )

    if strict:
        rows = (row for row in rows if row["language"] in language_set)

    writer.writerows(rows)


def resolve_output_file(output_file: str, mode="a") -> IO:

    output_is_stdout = bool(not output_file or output_file == "-")

    if output_is_stdout:
        return sys.stdout
    else:
        abs_output = os.path.abspath(output_file)

        return open(abs_output, mode, encoding="utf-8")


conll_type_to_wikidata_id = {"PER": "Q5", "LOC": "Q82794", "ORG": "Q43229"}


@click.command()
@click.option("--mongodb-uri", default="", help="MongoDB URI")
@click.option("--database-name", default="wikidata_db", help="Database name")
@click.option(
    "--collection-name", default="wikidata_simple", help="Collection name"
)
@click.option(
    "--subclass-coll-name",
    default="subclasses",
    help="Subclass collection name",
)
@click.option(
    "--output-format",
    "-f",
    type=click.Choice(["jsonl", "csv"]),
    default="jsonl",
)
@click.option(
    "--output-file",
    "-o",
    default="-",
    help="Output file. If empty or '-', defaults to stdout.",
)
@click.option(
    "--delimiter",
    "-d",
    type=click.Choice([",", "\t"]),
    default=",",
    help="Delimiter for CSV output. Can be comma or tab. Defaults to comma.",
)
@click.option(
    "--conll-type",
    "-t",
    type=click.Choice([t for t in conll_type_to_wikidata_id]),
    default=",",
)
@click.option(
    "--languages",
    "-l",
    default="en",
    help="Comma-separated list of languages to include",
)
@click.option(
    "--ids",
    "-i",
    default="",
    help="Only search for these IDs"
)
@click.option(
    "--num-docs",
    "-n",
    type=float,
    default=math.inf,
    help="Number of documents to output",
)
@click.option(
    "--strict",
    "-s",
    is_flag=True,
    help="Strict mode: Only output transliterations in languages specified using the -l flag.",
)
def main(
    mongodb_uri,
    database_name,
    collection_name,
    subclass_coll_name,
    output_format,
    output_file,
    delimiter,
    conll_type,
    languages,
    ids,
    num_docs,
    strict,
):

    # parse some input args
    language_list = languages.split(",")
    id_list = ids.split(",")
    output = output_jsonl if output_format == "jsonl" else output_csv

    # form connections to mongo db
    client = MongoClient(mongodb_uri) if mongodb_uri else MongoClient()
    subclasses = client[database_name][subclass_coll_name]
    db = client[database_name][collection_name]

    # formulate a list of all valid instance-of classes
    valid_instance_ofs = subclasses.find_one(
        {"id": conll_type_to_wikidata_id[conll_type]}
    )["subclasses"]

    # fetch results from mongodb
    filter_dict = {"instance_of": {"$in": valid_instance_ofs}}

    if languages:
        filter_dict["languages"] = {"$in": language_list}

    if ids:
        filter_dict["id"] = {"$in": id_list}

    results = (doc for doc in db.find(filter_dict))

    with resolve_output_file(output_file) as fout:
        for ix, doc in enumerate(results):
            if ix < num_docs:
                output(
                    wh.WikidataRecord(doc, simple=True),
                    f=fout,
                    languages=language_list,
                    strict=strict,
                    row_number=ix,
                )
            else:
                break


if __name__ == "__main__":
    main()
