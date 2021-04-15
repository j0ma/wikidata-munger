import bz2
import json
import orjson
import math
import os
import itertools
from unicodeblock import blocks
from collections import Counter
from functools import lru_cache

from typing import Generator, Set, List, Union, Dict, Any, IO, Tuple
from pymongo import MongoClient

import unicodedata as ud
import pandas as pd


@lru_cache(maxsize=None)
def unicode_blocks(word: str) -> Counter:
    return Counter(blocks.of(c) for c in str(word))  # wrap in str() to handle e.g. digits


@lru_cache(maxsize=None)
def most_common_unicode_block(word: str) -> str:
    return unicode_blocks(word).most_common(1)[0][0]


def read(input_file: str, io_format: str, typ: str = "frame") -> pd.DataFrame:
    if io_format in ["csv", "tsv"]:
        return pd.read_csv(
            input_file,
            encoding="utf-8",
            delimiter="\t" if io_format == "tsv" else ",",
        )
    elif io_format == "jsonl":
        return pd.read_json(input_file, "records", encoding="utf-8", typ=typ)
    elif io_format == "json":
        return pd.read_json(input_file, encoding="utf-8", typ=typ)


def write(
    data: pd.DataFrame, output_file: str, io_format: str, index: bool = False
) -> None:
    if io_format in ["csv", "tsv"]:
        return data.to_csv(
            output_file,
            sep="\t" if io_format == "tsv" else ",",
            encoding="utf-8",
            index=index,
        )
    else:
        return data.to_json(
            output_file, "records", encoding="utf-8", index=False
        )


class LatinChecker:
    """Cache-based checker for whether a string is Latin-only]

    Note: Very much based on https://anon.to/gSQN9s
    """

    def __init__(self):
        self.latin_letters = {}

    def is_latin(self, uchr):
        try:
            return self.latin_letters[uchr]
        except KeyError:
            return self.latin_letters.setdefault(
                uchr, "LATIN" in ud.name(uchr)
            )

    def only_latin_chars(self, unistr):
        return all(
            self.is_latin(uchr) for uchr in str(unistr) if uchr.isalpha()
        )  # isalpha suggested by John Machin

    def __call__(self, string):
        return self.only_latin_chars(string)


def english_dissimilarity(df: pd.DataFrame) -> Tuple[int, int]:
    """Computes a measure of 'english_dissimilarity', defined as
    N_good / N_tot, where

        - N_good: Wikidata IDs for which either
                    a) the English name and alias are not equal, or
                    b) there is no English name at all

        - N_tot: Total number of Wikidata IDs

    Returns a tuple (N_good, N_tot).

    Note: english_dissimilarity is easily computed as 1 - N_good / N_tot
    """

    # create masks
    english_alias_not_equal = df["alias"] != df["name"]
    no_english_name = df["name"] == df["id"]

    # compute quantities
    N_good = (english_alias_not_equal | no_english_name).sum()
    N_tot = df.shape[0]

    return N_good, N_tot


def compute_english_dissimilarity_df(csv: pd.DataFrame) -> pd.DataFrame:
    """Transform data frame of aliases to a data frame of english_dissimilarity scores"""

    english_dissimilarity_tuples = (
        csv.groupby(["language"]).apply(english_dissimilarity).reset_index()
    )
    out = english_dissimilarity_tuples[["language"]]
    out["n_good"] = english_dissimilarity_tuples[0].apply(lambda t: t[0])
    out["n_tot"] = english_dissimilarity_tuples[0].apply(lambda t: t[1])
    out["english_dissimilarity"] = out.n_good / out.n_tot
    out["english_similarity"] = 1 - out.english_dissimilarity

    return out


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

    def open_dump_file(self, dumpfile) -> IO[Any]:
        _, dumpfile_ext = os.path.splitext(dumpfile)

        if dumpfile_ext == ".bz2":
            return bz2.open(dumpfile, mode="rt")
        elif dumpfile_ext == ".json":
            return open(dumpfile, mode="r", encoding="utf-8")
        else:
            raise ValueError("Dump file must be .json or .bz2")

    def __iter__(self) -> Generator[Dict[str, Any], None, None]:
        with self.open_dump_file(self.dumpfile) as f:
            f.read(2)  # skip first two bytes: "{\n"

            for line in f:
                try:
                    yield orjson.loads(line.rstrip(",\n"))
                except orjson.JSONDecodeError:
                    self.n_decode_errors += 1

                    continue


class WikidataRecord:
    def __init__(
        self, record: dict, default_lang: str = "en", simple: bool = False
    ) -> None:
        self.simple = simple
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
        if self.simple:
            self.instance_ofs = self.record["instance_of"]
        else:
            try:
                self.instance_ofs = set(
                    iof["mainsnak"]["datavalue"]["value"]["id"]

                    for iof in self.record["claims"]["P31"]
                )
            except KeyError:
                self.instance_ofs = set()

    def parse_aliases(self) -> None:
        if self.simple:
            self.aliases = self.record["aliases"]
        else:
            self.aliases = {
                lang: d["value"] for lang, d in self.record["labels"].items()
            }

    def parse_alias_langs(self) -> None:
        if self.simple:
            self.alias_langs = self.record["languages"]
        else:
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

    def to_dict(self, simple=False) -> dict:
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
        simple: bool = False,
    ) -> Generator[Union[Dict[str, Any], WikidataRecord], None, None]:
        """Generator to yield at most n documents matching conditions in filter_dict."""

        if filter_dict is None:
            # by default, find everything that is an instance of something
            filter_dict = {"claims.P31": {"$exists": True}}

        for ix, doc in enumerate(self.collection.find(filter_dict)):
            if ix < n:
                yield WikidataRecord(doc, simple=simple) if as_record else doc
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
    ) -> None:

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
        self.cache: List[Union[str, Dict[Any, Any]]] = []

        # misc attributes
        self.max_docs = max_docs
        self.n_decode_errors = 0
        self.debug = debug
        self.simple_records = simple_records

    def establish_mongo_client(self, client) -> None:
        self.client = client
        self.db = self.client[self.database_name][self.collection_name]

    def write(self) -> None:
        """Writes cache contents (JSON list) to MongoDB"""

        if self.cache:
            if self.debug:
                print(f"Worker {self.name} inserting to MongoDB...")
            self.db.insert_many(self.cache)
            self.cache = []
            self.cache_used = len(self.cache)
        else:
            print(f"Cache empty for worker {self.name}. Not writing...")

    @property
    def cache_full(self) -> bool:
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

    def error_summary(self) -> None:
        print(
            f"Worker {self.name}, JSON decode errors: {self.n_decode_errors}"
        )

    def __call__(self) -> None:
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
