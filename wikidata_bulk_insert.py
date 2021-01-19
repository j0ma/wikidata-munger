#!/usr/bin/env python3

import math
from typing import Generator, Set, List

import click
from qwikidata.sparql import get_subclasses_of_item
from pymongo import MongoClient
from wikidata_helpers import WikidataDump
import itertools


def chunks(iterable, size):
    """Source: https://alexwlchan.net/2018/12/iterating-in-fixed-size-chunks/"""
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


@click.command()
@click.option("--dump-file", "-d", help="Path to dump file")
@click.option("--chunk-size", "-c", type=int, help="Chunk size")
@click.option("--mongodb-uri", default="", help="URI for MongoDB database")
@click.option("--verbose", is_flag=True)
def main(dump_file, chunk_size, mongodb_uri, verbose,) -> None:
    """Performs a linear scan through the .bz2 dump and optionally inserts to MongoDB"""

    if not mongodb_uri:
        print(
            "WARNING: MongoDB path not found.\n"
            "Using default URI: mongodb://localhost:27017/"
        )

    # set up mongo db connection
    mongo_client = MongoClient()
    db = mongo_client.wikidata_db

    for chunk in chunks(WikidataDump(dump_file), chunk_size):
        db.wikidata.insert_many(chunk)

if __name__ == "__main__":
    main()
