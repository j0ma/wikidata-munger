#!/usr/bin/env python3

import math
from typing import Generator, Set, List
import multiprocessing as mp

import click
from wikidata_helpers import WikidataMongoIngesterWorker


@click.command()
@click.option("--dump-file", "-d", help="Path to dump file")
@click.option("--database-name", default="wikidata_db", help="Database name")
@click.option(
    "--collection-name", default="parallel_ingest_test", help="Collection name"
)
@click.option("--num-workers", "-w", type=int, help="Number of workers")
@click.option("--chunk-size", "-c", type=int, default=1000, help="Chunk size")
@click.option("--max-docs", "-m", type=int, help="Max number of documents to ingest")
def main(
    dump_file, database_name, collection_name, num_workers, chunk_size, max_docs,
) -> None:

    workers = [
        WikidataMongoIngesterWorker(
            name=f"WikidataMongoIngestWorker{i}",
            input_path=dump_file,
            database_name=database_name,
            collection_name=collection_name,
            read_every=num_workers,
            start_at=i,
        )
        for i in range(1, num_workers + 1)
    ]

    def read_single_worker(worker):
        worker()

    with mp.Pool(processes=num_workers) as pool:
        pool.map(read_single_worker, workers)


if __name__ == "__main__":
    main()
