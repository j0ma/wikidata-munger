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
