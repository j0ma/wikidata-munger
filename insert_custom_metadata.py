#!/usr/bin/env python3

import multiprocessing as mp
from functools import partial
from typing import Generator, List, Iterable, Tuple, Dict

import click
from qwikidata.sparql import get_subclasses_of_item
from pymongo import MongoClient
from wikidata_helpers import WikidataMongoDB, WikidataRecord, chunks
from bson.objectid import ObjectId

"""
Adds custom metadata to each Wikidata dump record at the top level.
Mainly useful as a preprocessing step for indexing.

The default information added is
    a) instance-of
    b) languages in which there exists a transliteration/alias
"""


def grab_metadata_from_chunk(
    records: Iterable[WikidataRecord],
) -> List[Tuple[str, Dict[str, List[str]]]]:
    return [
        (
            record.mongo_id,
            {
                "instance_of": list(record.instance_ofs),
                "languages": list(record.languages),
            },
        )
        for record in records
    ]


def parallel_upsert(
    chunk_plus_id_tuple, verbose, database_name, collection_name, chunk_size
):
    chunk_id, chunk = chunk_plus_id_tuple
    wdb = WikidataMongoDB(database_name=database_name, collection_name=collection_name)
    if verbose:
        _from = chunk_id * chunk_size
        _to = (chunk_id + 1) * chunk_size - 1
        print(f"Updating documents {_from} - {_to}...")

    chunk = grab_metadata_from_chunk(chunk)
    for _id, doc in chunk:
        wdb.collection.update_one({"_id": ObjectId(_id)}, {"$set": doc}, upsert=True)


@click.command()
@click.option("--database-name", "-db", default="wikidata_db", help="Database name")
@click.option("--collection-name", "-c", default="wikidata", help="Collection name")
@click.option("--chunk-size", "-cz", type=int, help="Chunk size")
@click.option("--n-processes", "-np", type=int, default=8, help="Number of processes")
@click.option(
    "--max-tasks-per-child", "-m", type=int, default=1000, help="Max tasks per child"
)
@click.option("--verbose", is_flag=True)
def main(
    database_name,
    collection_name,
    chunk_size,
    n_processes,
    max_tasks_per_child,
    verbose,
) -> None:
    """Retrieves everything that is an instance of something from MongoDB,
    adds that information at the top level, and upserts the document"""

    outer_wdb = WikidataMongoDB(
        database_name=database_name, collection_name=collection_name
    )

    chunks_iterable = chunks(
        outer_wdb.find_matching_docs(as_record=True), chunk_size, should_enumerate=True
    )

    _parallel_upsert = partial(
        parallel_upsert,
        verbose=verbose,
        database_name=database_name,
        collection_name=collection_name,
        chunk_size=chunk_size,
    )

    with mp.Pool(processes=n_processes, maxtasksperchild=max_tasks_per_child) as pool:
        pool.map(_parallel_upsert, chunks_iterable)


if __name__ == "__main__":
    main()
