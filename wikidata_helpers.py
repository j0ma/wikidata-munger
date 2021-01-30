import bz2
import json
import orjson
import math
import os
import itertools

from typing import Generator, Set, List, Union, Dict, Any, TextIO
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
        self.n_decode_errors = 0

    def open_dump_file(self, dumpfile) -> TextIO:
        _, dumpfile_ext = os.path.splitetx(dumpfile)

        if dumpfile_ext == ".bz2":
            return bz2.open(dumpfile, mode="rt")
        elif dumpfile_ext == ".json":
            return open(dumpfile, mode="r", encoding="utf-8")
        else:
            raise ValueError("Dump file must be .json or .bz2")

    def __iter__(self) -> Generator:
        with self.open_dump_file(self.dumpfile) as f:
            f.read(2)  # skip first two bytes: "{\n"

            for line in f:
                try:
                    yield orjson.loads(line.rstrip(",\n"))
                except orjson.JSONDecodeError:
                    self.n_decode_errors += 1

                    continue


class WikidataRecord:
    def __init__(self, record: dict, default_lang: str = "en") -> None:
        self.record = record
        self.default_lang = default_lang
        self.parse_ids()
        self.parse_instance_of()
        self.parse_aliases()
        self.parse_alias_langs()
        self.parse_ipa()

    def parse_ids(self) -> None:
        self.id = self.record["id"]
        try:
            self.mongo_id = self.record["_id"]
        except KeyError:
            self.mongo_id = None

    def parse_instance_of(self) -> None:
        try:
            self.instance_ofs = set(
                iof["mainsnak"]["datavalue"]["value"]["id"]

                for iof in self.record["claims"]["P31"]
            )
        except KeyError:
            self.instance_ofs = set()

    def parse_aliases(self) -> None:
        self.aliases = {
            lang: d["value"] for lang, d in self.record["labels"].items()
        }

    def parse_alias_langs(self) -> None:
        self.alias_langs = {lang for lang in self.aliases}

    def parse_ipa(self) -> None:
        pass

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
            return {
                "id": self.id,
                "name": self.name,
                "aliases": self.aliases,
                "instance_of": list(self.instance_ofs),
                "languages": list(self.alias_langs),
            }
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
        self,
        database_name: str = "wikidata_db",
        collection_name: str = "wikidata",
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


class WikidataMongoIngesterWorker:
    """Class to handle reading every Nth line of Wikidata
    and ingesting them to MongoDB."""

    def __init__(
        self,
        name: str,
        input_path: str,
        database_name: str,
        collection_name: str,
        read_every: int = 1,
        start_at: int = 0,
        cache_size: int = 100,
        max_docs: Union[float, int] = math.inf,
        error_log_path: str = "",
        debug: bool = False,
        simple_records: bool = False,
    ):

        # naming and error logging related attributes
        self.name = name
        self.error_log_path = (
            error_log_path if error_log_path else f"/tmp/{self.name}.error.log"
        )

        # reading-related attributes
        self.input_path = input_path
        self.start_at = start_at
        self.next_read_at = start_at
        self.read_every = read_every

        # database-related attributes
        self.database_name = database_name
        self.collection_name = collection_name

        # caching-related attributes
        self.cache_size = cache_size
        self.cache_used = 0
        self.cache: List[str] = []

        # misc attributes
        self.max_docs = max_docs
        self.n_decode_errors = 0
        self.debug = debug
        self.simple_records = simple_records

    def establish_mongo_client(self, client):
        self.client = client
        self.db = self.client[self.database_name][self.collection_name]

    def write(self):
        """Writes cache contents (JSON list) to MongoDB"""

        if self.cache:
            if self.debug:
                print(f"Worker {self.name} inserting to MongoDB...")
            self.db.insert_many(self.cache)
            self.cache = []
            self.cache_used = len(self.cache)

    @property
    def cache_full(self):
        """The cache is defined to be full when its size
        is at least as large as self.cache_size."""

        if self.cache_used >= self.cache_size:
            if self.debug:
                print(
                    f"Cache full for worker {self.name}. Used: {self.cache_used}, Size: {self.cache_size}"
                )

            return True
        else:
            return False

    def __call__(self):
        """Main method for invoking the read procedure.

        Iterates over Wikidata JSON dump (decompressed),
        reads every Nth line starting from a given line,
        caches the ingested lines, and bulk inserts them
        to a specified MongoDB collection as required."""

        with open(self.input_path, encoding="utf-8") as f:
            for line_nr, line in enumerate(f, start=1):

                # if we're too early, skip

                if line_nr < self.start_at:
                    continue

                # if we're past the max, stop

                if line_nr > self.max_docs:
                    self.write()

                    break

                # if we're exactly at the right spot, parse JSON

                if line_nr == self.next_read_at:
                    try:
                        doc = orjson.loads(line.rstrip(",\n"))
                        record = WikidataRecord(doc)
                        self.cache.append(
                            record.to_dict(simple=self.simple_records)
                        )
                        self.cache_used += 1
                    except orjson.JSONDecodeError:
                        # in case of decode error, log it and keep going
                        self.n_decode_errors += 1

                        continue

                    # in either case, take note of next line to read at
                    self.next_read_at += self.read_every

                # always write if our cache is full

                if self.cache_full:
                    self.write()

            # finally write one more time
            self.write()
