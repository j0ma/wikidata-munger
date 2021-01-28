import bz2
import json
import orjson
import math
import os
import itertools

from typing import Generator, Set, Union, Dict, Any
from pymongo import MongoClient


def orjson_dump(d: dict) -> str:
    """Dumps a dictionary in UTF-8 format using orjson"""
    json_bytes: bytes = orjson.dumps(d)
    json_utf8 = json_bytes.decode("utf8")
    return json_utf8

def json_dump(d: dict) -> str:
    """Dumps a dictionary in UTF-8 foprmat using json

    Effectively the `json` counterpart to `orjson_dump`.
    """
    return json.dumps(d, ensure_ascii=False)

def chunks(iterable, size, should_enumerate=False):
    """Source: https://alexwlchan.net/2018/12/iterating-in-fixed-size-chunks/"""
    it = iter(iterable)
    ix = 0
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield (ix, chunk) if should_enumerate else chunk
        ix += 1


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
        self.parse_ids()
        self.parse_instance_of()
        self.parse_aliases()
        self.parse_alias_langs()

    def parse_ids(self) -> None:
        self.id = self.record["id"]
        self.mongo_id = self.record["_id"]

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

    def parse_alias_langs(self) -> None:
        self.alias_langs = {lang for lang in self.aliases}

    @property
    def name(self) -> str:
        try:
            if not hasattr(self, "_name"):
                self._name = self.aliases[self.default_lang]

            return self._name
        except KeyError:
            return self.id

    @property
    def languages(self) -> Set[str]:
        """Returns a set of languages in which the entity has a transliteration."""
        return self.alias_langs

    def instance_of(self, classes: Set[str]) -> bool:
        """Checks whether the record is an instance of a set of classes"""

        return len(self.instance_ofs.intersection(classes)) > 0

    def to_dict(self, simple=False, custom_metadata=False) -> dict:
        if simple:
            return {"id": self.id, "name": self.name, "aliases": self.aliases}
        else:
            return self.record

    def to_json(self, simple=True) -> str:
        return orjson_dump(self.to_dict(simple))

    def __str__(self) -> str:
        return f'WikidataRecord(name="{self.name}", id="{self.id}, mongo_id={self.mongo_id} instance_of={self.instance_ofs})"'

    def __repr__(self) -> str:
        return str(self)


class WikidataMongoDB:
    """Class for interfacing with Wikidata dump ingested into a MongoDB instance."""

    def __init__(
        self, database_name: str = "wikidata_db", collection_name: str = "wikidata",
    ) -> None:
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = MongoClient()
        self.collection = self.client[self.database_name][self.collection_name]

    def find_matching_docs(
        self,
        filter_dict: Union[dict, None] = None,
        n: Union[float, int] = math.inf,
        as_record: bool = False,
    ) -> Generator[Union[Dict[str, Any], WikidataRecord], None, None]:
        """Generator to yield at most n documents matching conditions in filter_dict."""
        if filter_dict is None:
            # by default, find everything that is an instance of something
            filter_dict = {"claims.P31": {"$exists": True}}
        for ix, doc in enumerate(self.collection.find(filter_dict)):
            if ix < n:
                yield WikidataRecord(doc) if as_record else doc
            else:
                break
