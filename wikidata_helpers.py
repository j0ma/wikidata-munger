import bz2
import orjson
import os

from typing import Generator, Set, List


class WikidataDump:
    def __init__(self, dumpfile: str) -> None:
        self.dumpfile = os.path.abspath(dumpfile)

    def __iter__(self) -> Generator:
        n_decode_errors = 0
        with bz2.open(self.dumpfile, mode="rt") as f:
            f.read(2)  # skip first two bytes: "{\n"
            for line in f:
                try:
                    yield orjson.loads(line.rstrip(",\n"))
                except orjson.JSONDecodeError:
                    n_decode_errors += 1
                    continue
        print(f"Decode errors: {n_decode_errors}")
            


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
        return orjson.dumps(self.to_dict(simple), ensure_ascii=False)

    def __str__(self) -> str:
        return f'WikidataRecord(name="{self.name}", id="{self.id})"'

    def __repr__(self) -> str:
        return str(self)
