import sys
import os
import math
import csv
from typing import IO, Generator, List, Dict, Any, Union, Iterable

from pymongo import MongoClient
import wikidata_helpers as wh
import click


def output_jsonl(
    documents: List[Dict[str, str]],
    f: IO,
    languages: Iterable[str],
    conll_type: str,
    strict: bool = False,
    row_number: int = 0,
) -> None:

    language_set = set(languages)

    for doc in documents:
        if strict and doc["language"] not in language_set:
            continue
        doc_str = wh.orjson_dump(doc)
        f.write(f"{doc_str}\n")


def output_csv(
    documents: List[Dict[str, str]],
    f: IO,
    languages: Iterable[str],
    conll_type: str,
    strict: bool = False,
    row_number: int = 0,
    delimiter: str = ",",
) -> None:
    writer = csv.DictWriter(f, fieldnames=["language", "conll_type", "num_docs"])
    language_set = set(languages)

    if row_number == 0:
        writer.writeheader()

    if strict:
        documents = (doc for doc in docs if doc["language"] in language_set)

    writer.writerows(documents)


def resolve_output_file(output_file: str, mode="a") -> IO:

    output_is_stdout = bool(not output_file or output_file == "-")

    if output_is_stdout:
        return sys.stdout
    else:
        abs_output = os.path.abspath(output_file)

        return open(abs_output, mode, encoding="utf-8")


conll_type_to_wikidata_id = {"PER": "Q5", "LOC": "Q82794", "ORG": "Q43229"}


def get_subclasses_per_type(subclasses, conll_type) -> List[str]:
    return subclasses.find_one({"id": conll_type_to_wikidata_id[conll_type]})[
        "subclasses"
    ]


# TODO: type annotate
def count_entities_per_type(db, conll_type, filter_dict):

    results = db.aggregate(
        [
            {"$match": filter_dict},
            {"$unwind": "$languages"},
            {"$project": {"_id": 1, "language": "$languages"}},
            {"$group": {"_id": "$language", "nDocs": {"$sum": 1}}},
            {
                "$project": {
                    "language": "$_id",
                    "num_docs": "$nDocs",
                    "_id": 0,
                }
            },
        ]
    )

    output = [{**res, "conll_type": conll_type} for res in results]

    print(output[0])

    return output


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
# @click.option(
    # "--conll-type",
    # "-t",
    # type=click.Choice([t for t in conll_type_to_wikidata_id]),
    # default=",",
    # required=False
# )
@click.option(
    "--languages",
    "-l",
    default="en",
    help="Comma-separated list of languages to include",
)
@click.option("--ids", "-i", default="", help="Only search for these IDs")
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
    # conll_type,
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

    results = []

    for conll_type in conll_type_to_wikidata_id:

        # formulate a list of all valid instance-of classes
        valid_instance_ofs = get_subclasses_per_type(subclasses, conll_type)

        # fetch results from mongodb
        filter_dict = {"instance_of": {"$in": valid_instance_ofs}}
        result = count_entities_per_type(db, conll_type, filter_dict)
        results.extend(result)

    with resolve_output_file(output_file) as fout:
        output(
            results,
            f=fout,
            languages=language_list,
            conll_type=conll_type,
            strict=strict,
            row_number=0,
        )


if __name__ == "__main__":
    main()
