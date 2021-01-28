#!/usr/bin/env python3

"""
Get Wikidata dump records as a JSON stream (one JSON object per line)

Based on https://anon.to/okOU3G
"""

import math
from typing import Generator, Set

import click
from qwikidata.sparql import get_subclasses_of_item
from pymongo import MongoClient
from wikidata_helpers import WikidataDump, WikidataRecord


@click.command()
@click.option("--dump-file", "-d", help="Path to dump file")
@click.option(
    "--num-records",
    "-n",
    type=float,
    default=math.inf,
    help="Number of records to print",
)
@click.option(
    "--instance-subclass-of",
    default="Q5",
    help="Identifier for instance-of/subclass-of relation",
)
@click.option("--insert-to-mongodb", is_flag=True)
@click.option("--mongodb-uri", default="", help="URI for MongoDB database")
@click.option("--verbose", is_flag=True)
def main(
    dump_file,
    num_records,
    instance_subclass_of,
    insert_to_mongodb,
    mongodb_uri,
    verbose,
) -> None:
    """Performs a linear scan through the .bz2 dump and optionally inserts to MongoDB"""

    if insert_to_mongodb and not mongodb_uri:
        print(
            "WARNING: MongoDB path not found.\n"
            "Using default URI: mongodb://localhost:27017/"
        )

    if insert_to_mongodb:

        def process(record: WikidataRecord, verbose: bool = True) -> None:
            mongo_client = MongoClient()
            db = mongo_client.wikidata_db
            wikidata = db.wikidata
            if verbose:
                print(f"Inserting {record}")
            wikidata.insert_one(record.to_dict(simple=False))

    else:

        def process(record: WikidataRecord, verbose: bool = True) -> None:
            print(record.to_json())

    subclasses = set(get_subclasses_of_item(instance_subclass_of))

    n_dumped = 0
    for record in WikidataDump(dump_file):

        record = WikidataRecord(record)

        if n_dumped >= num_records:
            break

        if record.instance_of(subclasses):
            process(record, verbose=verbose)
            n_dumped += 1


if __name__ == "__main__":
    main()
