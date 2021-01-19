#!/usr/bin/env python3

"""
Get Wikidata dump records as a JSON stream (one JSON object per line)

Based on https://anon.to/okOU3G
"""

import bz2
import json
import os
import math
from typing import Generator, Set

import click
from qwikidata.sparql import get_subclasses_of_item
from pymongo import MongoClient


class WikidataDump:
    def __init__(self, dumpfile: str) -> None:
        self.dumpfile = os.path.abspath(dumpfile)

    def __iter__(self) -> Generator:
        with bz2.open(self.dumpfile, mode="rt") as f:
            f.read(2)  # skip first two bytes: "{\n"
            for line in f:
                try:
                    yield json.loads(line.rstrip(",\n"))
                except json.decoder.JSONDecodeError:
                    continue


class WikidataRecord:
    def __init__(self, record: dict, default_lang: str = "en") -> None:
        self.record = record
        self.default_lang = default_lang
        self.parse_id()
        self.parse_instance_of()
        self.parse_aliases()

    def parse_id(self) -> None:
        self.id = self.record["id"]

    def parse_instance_of(self) -> None:
        try:
            self.instance_ofs = set(
                iof["mainsnak"]["datavalue"]["value"]["id"]
                for iof in self.record["claims"]["P31"]
            )
        except KeyError:
            self.instance_ofs = set()

    def parse_aliases(self) -> None:
        self.aliases = {lang: d["value"] for lang, d in self.record["labels"].items()}

    @property
    def name(self) -> str:
        try:
            if not hasattr(self, "_name"):
                self._name = self.aliases[self.default_lang]
            return self._name
        except KeyError:
            return self.id

    def instance_of(self, classes: Set[str]) -> bool:
        """Checks whether the record is an instance of a set of classes"""
        return len(self.instance_ofs.intersection(classes)) > 0

    def to_dict(self, simple=True) -> dict:
        if simple:
            return {"id": self.id, "name": self.name, "aliases": self.aliases}
        else:
            return self.record

    def to_json(self, simple=True) -> str:
        return json.dumps(self.to_dict(simple), ensure_ascii=False)

    def __str__(self) -> str:
        return f'WikidataRecord(name="{self.name}", id="{self.id})"'

    def __repr__(self) -> str:
        return str(self)


@click.command()
@click.option("--dump-file", "-d", help="Path to dump file")
@click.option(
    "--num-records",
    "-n",
    type=float,
    default=math.inf,
    help="Number of records t o print",
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
    dump_file, num_records, instance_subclass_of, insert_to_mongodb, mongodb_uri, verbose
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
