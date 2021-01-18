#!/usr/bin/env python3

"""
Get Wikidata dump records as a JSON stream (one JSON object per line)

Based on https://anon.to/okOU3G
"""

import bz2
import json
import os
import math

import click

from typing import Generator, Dict


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
            self.instance_ofs = {
                iof["mainsnak"]["datavalue"]["value"]["id"]
                for iof in self.record["claims"]["P31"]
            }
        except KeyError:
            self.instance_ofs = set()

    def parse_aliases(self):
        self.aliases = {lang: d["value"] for lang, d in self.record["labels"].items()}

    @property
    def name(self) -> str:
        if not hasattr(self, "_name"):
            self._name = self.aliases[self.default_lang]
        return self._name

    def instance_of(self, identifier: str) -> bool:
        return identifier in self.instance_ofs

    def to_json(self, simple=True) -> str:
        if simple:
            return json.dumps(
                {"id": self.id, "name": self.name, "aliases": self.aliases},
                ensure_ascii=False,
            )
        else:
            return json.dumps(self.record, ensure_ascii=False)

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
    help="Number of records to print",
)
@click.option("--instance-of", default="Q5", help="Identifier for instance-of relation")
def main(dump_file, num_records, instance_of):

    n_dumped = 0
    wikidata = WikidataDump(dump_file)
    for record in wikidata:
        if n_dumped >= num_records:
            break

        record = WikidataRecord(record)

        if record.instance_of(instance_of):
            print(record.to_json())
            n_dumped += 1


if __name__ == "__main__":
    main()
