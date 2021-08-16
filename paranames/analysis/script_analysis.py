from functools import lru_cache
from collections import Counter
from typing import (
    Generator,
    Set,
    List,
    Union,
    Dict,
    Any,
    IO,
    Tuple,
    Type,
    Callable,
    Iterable,
    Optional,
)
from pathlib import Path
import itertools as it
import tempfile as tf
import subprocess
import sys
import csv
import re
import io
import os

import unicodedata as ud
from unicodeblock import blocks
from tqdm import tqdm
from p_tqdm import p_map
import pandas as pd
import numpy as np
import attr

import editdistance

import dictances as dt

import multiprocessing as mp

CACHE_MAX_SIZE = 10000


def chunks(lst, n):
    """Yield successive n-sized chunks from lst.

    Note: taken from SO (https://anon.to/vc1xVW)
    """

    for i in range(0, len(lst), n):
        yield lst[i : i + n]


class UnicodeAnalyzer:
    def __init__(
        self,
        strip: bool = False,
        ignore_punctuation: bool = False,
        ignore_numbers: bool = False,
        normalize_histogram: bool = True,
    ) -> None:
        self.strip = strip
        self.ignore_punctuation = ignore_punctuation
        self.normalize_histogram = normalize_histogram
        self.ignore_numbers = ignore_numbers

    def is_punctuation(self, s: str) -> bool:
        is_punc = ud.category(s).startswith("P")
        is_symb = ud.category(s).startswith("S")

        return is_punc or is_symb

    def is_number(self, c: str) -> bool:
        return ud.category(c).startswith("N")

    def maybe_strip(self, word: str) -> str:
        return str(word).strip() if self.strip else str(word)

    # @lru_cache(maxsize=CACHE_MAX_SIZE)
    def unicode_blocks(self, word: str) -> Counter:
        punctuation_cond = (
            lambda w: not self.is_punctuation(str(w))

            if self.ignore_punctuation
            else True
        )

        digit_cond = (
            lambda w: not self.is_number(str(w))

            if self.ignore_numbers
            else True
        )

        return Counter(
            blocks.of(c)

            for c in self.maybe_strip(word)

            if blocks.of(c) and punctuation_cond(c) and digit_cond(c)
        )

    # @lru_cache(maxsize=CACHE_MAX_SIZE)
    def most_common_unicode_block(self, word: str) -> str:
        try:
            return self.unicode_blocks(word).most_common(1)[0][0]
        except IndexError:
            return ""

    # @lru_cache(maxsize=CACHE_MAX_SIZE)
    def unicode_block_histogram(
        self,
        word: str,
    ) -> Counter:
        histogram = self.unicode_blocks(word)

        if self.normalize_histogram:
            total = sum(histogram.values())

            for block, count in histogram.items():
                histogram[block] = count / total

        return histogram


@attr.s(frozen=False, slots=True, hash=True)
class TransliteratedName:
    text: str = attr.ib()
    is_unchanged: bool = attr.ib()
    language: str = attr.ib()
    unicode_analyzer: UnicodeAnalyzer = attr.ib(repr=False)
    wikidata_id: str = attr.ib(default="Q0")  # fake default
    conll_type: Optional[str] = attr.ib(default="NONE")  # fake default
    anomalous: Optional[bool] = attr.ib(default=None)
    noise_sample: Optional[bool] = attr.ib(default=False)
    english_text: Optional[str] = attr.ib(default=None)
    analyze_unicode: bool = attr.ib(default=True, repr=False)
    alignment: Optional["Alignment"] = attr.ib(default=None, repr=False)
    _original_text: Optional[str] = attr.ib(default=None, repr=False)

    @property
    def original_text(self) -> str:
        return self._original_text or self.text

    @original_text.setter
    def original_text(self, text) -> None:
        self._original_text = text

    @property
    def most_common_unicode_block(self) -> str:
        return self.unicode_analyzer.most_common_unicode_block(self.text)

    @property
    def unicode_block_histogram(self) -> Dict[str, float]:
        return self.unicode_analyzer.unicode_block_histogram(self.text)

    def __hash__(self) -> int:
        return hash(self.text + self.language)

    def add_alignment(self, alignment: "Alignment") -> None:
        self.alignment = alignment

    def to_dict(
        self,
        english_text_key: str = "eng",
        text_key: str = "alias",
        wikidata_id_key: str = "wikidata_id",
        conll_type_key: str = "type",
        unicode_block_key: str = "most_common_unicode_block",
        original_text_key: str = "original_text",
        language_key: str = "language",
        is_unchanged_key: str = "is_unchanged",
    ) -> Dict[str, str]:
        return {
            english_text_key: self.english_text or "",
            text_key: self.text,
            wikidata_id_key: self.wikidata_id,
            conll_type_key: self.conll_type or "",
            unicode_block_key: self.most_common_unicode_block,
            original_text_key: self.original_text or "",
            language_key: self.language,
            is_unchanged_key: self.is_unchanged,
        }

    def to_series(
        self,
        english_text_key: str = "eng",
        text_key: str = "alias",
        wikidata_id_key: str = "wikidata_id",
        conll_type_key: str = "type",
        unicode_block_key: str = "most_common_unicode_block",
        original_text_key: str = "original_text",
        language_key: str = "language",
    ):

        return pd.Series(
            self.to_dict(
                english_text_key,
                text_key,
                wikidata_id_key,
                conll_type_key,
                unicode_block_key,
                original_text_key,
                language_key,
            )
        )


@attr.s(frozen=False, slots=True)
class Alignment:

    alignment_str: str = attr.ib(repr=False)
    n_cross_alignments: int = attr.ib(repr=False, default=0)

    def __attrs_post_init__(
        self,
    ) -> None:

        self.alignment_str = self.alignment_str.strip()

        if self.alignment_str:
            self.n_cross_alignments = self.compute_cross_alignments()

    def compute_cross_alignments(self) -> int:
        n_cross_alignments = 0
        max_tgt_seen = 0

        alignment_tokens: List[Tuple[int, ...]] = sorted(
            [
                tuple([int(x) for x in at.split("-")])

                for at in self.alignment_str.split(" ")
            ],
            key=lambda t: t[0],
        )

        for tok in alignment_tokens:

            try:
                source, target = tok

                if target < max_tgt_seen:
                    n_cross_alignments += 1

                max_tgt_seen = max(max_tgt_seen, target)
            except:
                continue

        return n_cross_alignments


@attr.s
class NameWriter:

    out_folder: Union[str, Path] = attr.ib(default="")
    debug_mode: bool = attr.ib(default=False)

    def write(
        self,
        splits: Dict[str, List[TransliteratedName]],
        unicode_block_mode: bool = True,
        write_permutations_mode: bool = False,
        field_name_map: Optional[Dict[str, str]] = None,
    ) -> None:

        assert unicode_block_mode or write_permutations_mode

        if not field_name_map:
            field_name_map = {
                "wikidata_id": "wikidata_id",
                "conll_type": "type",
                "text": "alias",
                "english_text": "eng",
                "unicode_block": "most_common_unicode_block",
                "original_text": "original_text",
                "language": "language",
                "is_unchanged": "is_unchanged",
            }

        field_names = [
            field_name_map["wikidata_id"],
            field_name_map["language"],
            field_name_map["conll_type"],
            field_name_map["text"],
        ]

        if write_permutations_mode:
            field_names.extend(
                [
                    field_name_map["original_text"],
                    field_name_map["is_unchanged"],
                ]
            )
        else:
            field_names.append(field_name_map["unicode_block"])

        if not self.out_folder:
            self.out_folder = Path(tf.mkdtemp())

            if self.debug_mode:
                print(
                    f"[NameWriter] Output folder {self.out_folder} not found. Creating..."
                )
        else:
            self.out_folder = Path(self.out_folder)

            if not self.out_folder.exists():
                self.out_folder.mkdir(parents=True)

        extension = "txt" if unicode_block_mode else "tsv"

        for category, names in splits.items():
            path = self.out_folder / f"{category}.{extension}"

            with open(path, "w", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=field_names,
                    extrasaction="ignore",
                    delimiter="\t",
                )

                writer.writeheader()

                for name in tqdm(
                    sorted(names, key=lambda name: name.text), total=len(names)
                ):
                    writer.writerow(name.to_dict())

            if self.debug_mode:
                print(f"[{category}] Names written to {path}")


@attr.s(frozen=False, slots=True)
class CorpusStatistics:
    """Cross-alignment statistics for a transliterated name corpus

    Notes
    -----
    - if alignments is None, names is assumed to contain alignments
    - if names doesn't, then alignments must be passed
    """

    names: Iterable[TransliteratedName] = attr.ib(repr=False)
    alignments: Optional[Iterable[Optional[Alignment]]] = attr.ib(
        default=None, repr=False
    )

    mean_cross_alignments: float = attr.ib(default=0.0)
    total_cross_alignments: int = attr.ib(default=0)
    total_permuted: int = attr.ib(default=0)
    total_surviving: int = attr.ib(default=0)

    def __attrs_post_init__(self) -> None:

        self.alignments: Iterable[Optional[Alignment]] = (
            self.alignments

            if self.alignments
            else (n.alignment for n in self.names)
        )

        self.mean_cross_alignments = np.mean(
            [a.n_cross_alignments if a else 0 for a in self.alignments]
        )
        self.total_cross_alignments = sum(
            [a.n_cross_alignments if a else 0 for a in self.alignments]
        )

        self.total_permuted = sum(not n.is_unchanged for n in self.names)
        self.total_surviving = sum(n.is_unchanged for n in self.names)


@attr.s
class UniversalRomanizer:
    """Python binding to ISI's Universal Romanizer written in Perl

    Notes
    -----
    - By default, assumes that the uroman invocation command is stored
      in the UROMAN_CMD environment variable. It is also possible to pass
      the command as an argument to __init__.
    """

    uroman_cmd: str = attr.ib(default=os.environ["UROMAN_CMD"])
    num_workers: int = attr.ib(default=1)
    chunksize: int = attr.ib(default=10000)

    def __call__(self, strings: List[str]) -> List[str]:
        if self.num_workers == 1:
            return self.call(strings)
        else:
            return self.call_parallel(strings)

    def call_parallel(self, strings: List[str]) -> List[str]:
        return list(
            it.chain.from_iterable(
                p_map(
                    self.call,
                    chunks(strings, self.chunksize),
                    num_cpus=self.num_workers,
                )
            )
        )

    def call(self, strings: List[str]) -> List[str]:

        try:
            completed_pid = subprocess.run(
                [self.uroman_cmd],
                input="\n".join(strings),
                capture_output=True,
                encoding="utf-8",
                text=True,
            )

            uroman_output = [
                line

                for string, line in zip(
                    strings, completed_pid.stdout.split("\n")
                )
            ]

            assert len(uroman_output) == len(
                strings
            ), f"{len(uroman_output)} != {len(strings)}"

        except subprocess.CalledProcessError as err:
            print(err)
        except OSError as err:
            print(err)
            raise ValueError("uroman must be installed!")

        return uroman_output


@attr.s
class FastAligner:

    verbose: bool = attr.ib(repr=False, default=False)
    preserve_raw_output: bool = attr.ib(repr=False, default=False)
    debug_mode: bool = attr.ib(repr=False, default=False)

    def load_alignment_file(
        self,
        alignment_file: Union[Path, str],
        names: Optional[Iterable[TransliteratedName]],
    ) -> List[Alignment]:
        with open(alignment_file, "r", encoding="utf-8") as f:

            alignments = self.load_alignments_from_iterable(iterable=f)

            return alignments

    def load_alignments_from_iterable(
        self,
        iterable: Iterable[str],
    ) -> List[Alignment]:

        alignments = []

        for alignment_string in tqdm(iterable):
            a = Alignment(alignment_str=alignment_string)
            alignments.append(a)

        return alignments

    def __call__(
        self, names: List[TransliteratedName]
    ) -> Tuple[List[Alignment], List[TransliteratedName]]:
        """Performs a call to fast_align in a separate process.

        Note: assumes that all names have already been filtered,
              so no extensive checking is performed to validate
              that names and fast_align output strings match"""

        alignment_train_data = Path(tf.mktemp())
        fastalign_output = Path(tf.mktemp())

        if self.verbose:
            print(
                f"[FastAligner] Saving alignment training data to: {alignment_train_data}"
            )
            print(
                f"[FastAligner] Saving fast_align output to: {fastalign_output}"
            )

        # first we write our words into a temporary file for fast_align
        with open(alignment_train_data, "w") as f_out:
            for name in names:

                # if name.english_text and name_text:
                name_text = name.text.replace(" ", "▁").strip()
                english_text = name.english_text.replace(" ", "▁")
                f_out.write(
                    f"{' '.join(name_text)} ||| {' '.join(english_text)}\n"
                )

        # then perform the actual call to fast_align
        try:
            with open(fastalign_output, "w", encoding="utf-8") as f_out, open(
                "/dev/null", "w"
            ) as null:

                fastalign_completed_pid = subprocess.run(
                    [
                        "fast_align",
                        "-v",
                        "-d",
                        "-o",
                        "-i",
                        alignment_train_data,
                    ],
                    capture_output=True,
                    encoding="utf-8",
                    text=True,
                )

                fastalign_stdout = [
                    line

                    for name, line in zip(
                        names, fastalign_completed_pid.stdout.split("\n")
                    )
                ]

        except subprocess.CalledProcessError as err:
            print(err)
        except OSError as err:
            print(err)
            raise ValueError("fast_align must be installed!")

        # get all the alignments out as a collection
        alignments = self.load_alignments_from_iterable(
            iterable=fastalign_stdout
        )

        # then link the names with the alignments

        if self.debug_mode:
            print("Adding alignments to names...")
            name_alignment_iter = tqdm(
                zip(names, alignments), total=len(names)
            )
        else:
            name_alignment_iter = zip(names, alignments)

        for name, alignment in name_alignment_iter:
            name.add_alignment(alignment)

        # make sure we have as many alignments as we have names
        assert len(alignments) == len(names)

        # finally remove the temporary files unless told otherwise

        if not self.preserve_raw_output:
            alignment_train_data.unlink()
            fastalign_output.unlink()

        return alignments, names


@attr.s
class NameProcessor:

    inplace: bool = attr.ib(default=False)
    debug_mode: bool = attr.ib(default=False)
    num_workers: int = attr.ib(default=1, repr=False)

    def process(self, string: str) -> str:
        raise NotImplementedError

    def __call__(
        self, names: List[TransliteratedName]
    ) -> List[TransliteratedName]:
        if self.inplace:
            return self._call_inplace(names)
        else:
            return self._call_immutable(names)

    def _call_inplace(
        self, names: List[TransliteratedName]
    ) -> List[TransliteratedName]:

        for name in names:
            processed_name = self.process(name.text)

            if self.debug_mode and name.text != processed_name:
                print(
                    f"[{name.english_text}] {name.text} => {processed_name}".strip()
                )
            name.is_unchanged = bool(name.text == processed_name)
            name.original_text = name.text
            name.text = processed_name

        return names

    def _call_immutable(
        self, names: List[TransliteratedName]
    ) -> List[TransliteratedName]:

        output = []

        for name in names:
            processed_name = self.process(name.text)

            if self.debug_mode and name.text != processed_name:
                print(
                    f"[{name.english_text}] {name.text} => {processed_name}".strip()
                )
            output.append(
                TransliteratedName(
                    text=processed_name,
                    language=name.language,
                    unicode_analyzer=name.unicode_analyzer,
                    anomalous=name.anomalous,
                    noise_sample=name.noise_sample,
                    english_text=name.english_text,
                    alignment=name.alignment,
                    original_text=name.text,
                    is_unchanged=bool(name.text == processed_name),
                )
            )

        return output


@attr.s
class ParenthesisRemover(NameProcessor):

    re_parenthesis = re.compile(r"\(.*\)")

    def process(self, string: str) -> str:
        return self.re_parenthesis.sub("", string).strip()


@attr.s
class PermuteFirstComma(NameProcessor):
    """Permutes tokens around the first comma (,) if one is present.

    Meant to catch cases like 'Biden, Joe' => 'Joe Biden'
    """

    comma: str = attr.ib(default=",")

    def process(self, name_text: str) -> str:
        comma_ix = name_text.find(self.comma)

        if comma_ix == -1:
            return name_text
        else:
            head = name_text[:comma_ix]
            tail = name_text[(comma_ix + 1) :]

            return f"{tail} {head}".strip(",").strip()


@attr.s
class RemoveParenthesisPermuteComma(NameProcessor):
    """Composes ParenthesisRemover and CommaPermuter"""

    parenthesis_remover = ParenthesisRemover()
    comma_permuter = PermuteFirstComma()

    def process(self, name_text: str) -> str:
        processed = self.parenthesis_remover.process(name_text)
        processed = self.comma_permuter.process(processed)

        return processed.strip(",")


@attr.s
class PermuteLowestDistance(NameProcessor):
    """Permutes tokens in a name to achieve the lowest distance
    between the name and its romanized version.
    """

    distance_function: Callable[[str, str], float] = attr.ib(
        default=editdistance.eval
    )

    length_lower_bound: int = attr.ib(repr=False, default=2)
    length_upper_bound: int = attr.ib(repr=False, default=4)

    def __attrs_post_init__(self) -> None:
        self.romanizer = UniversalRomanizer(num_workers=self.num_workers)

    def romanize(self, names: List[str]) -> List[str]:
        return self.romanizer(names)

    def _call_immutable(
        self, names: List[TransliteratedName]
    ) -> List[TransliteratedName]:

        output = []

        romanized_names = self.romanize([n.text for n in names])

        for name, romanized_name in zip(names, romanized_names):
            tokens = name.text.split()
            romanized_tokens = romanized_name.split()
            orig_text = name.text

            # skip over names that have too few or too many tokens

            if not (
                self.length_lower_bound
                <= len(tokens)
                <= self.length_upper_bound
            ):
                output.append(name)

            # otherwise find the best permutation
            else:

                permuted = [" ".join(perm) for perm in it.permutations(tokens)]
                permuted_romanized = [
                    " ".join(perm)

                    for perm in it.permutations(romanized_tokens)
                ]

                best_distance = np.inf
                best_name = name.text

                for permutation, rom_permutation in zip(
                    permuted, permuted_romanized
                ):
                    ed = self.distance_function(
                        rom_permutation, name.english_text
                    )

                    if ed < best_distance:
                        best_distance = ed
                        best_name = permutation.strip(",").strip()

                        if self.debug_mode:
                            print(
                                f"[{name.english_text}] {best_name} (ed={best_distance})"
                            )

                output.append(
                    TransliteratedName(
                        text=best_name,
                        language=name.language,
                        unicode_analyzer=name.unicode_analyzer,
                        anomalous=name.anomalous,
                        noise_sample=name.noise_sample,
                        english_text=name.english_text,
                        is_unchanged=bool(best_name == orig_text),
                        original_text=orig_text,
                    )
                )

        return output

    def _call_inplace(
        self, names: List[TransliteratedName]
    ) -> List[TransliteratedName]:

        romanized_names = self.romanize([n.text for n in names])

        for name, romanized_name in zip(names, romanized_names):
            tokens = name.text.split()
            romanized_tokens = romanized_name.split()
            orig_text = name.text

            if not (
                self.length_lower_bound
                <= len(tokens)
                <= self.length_upper_bound
            ):
                continue

            permuted = [" ".join(perm) for perm in it.permutations(tokens)]
            permuted_romanized = [
                " ".join(perm) for perm in it.permutations(romanized_tokens)
            ]

            best_distance = np.inf
            best_name = name.text

            for permutation, rom_permutation in zip(
                permuted, permuted_romanized
            ):
                ed = self.distance_function(rom_permutation, name.english_text)

                if ed < best_distance:
                    best_distance = ed
                    best_name = permutation

            name.text = best_name
            name.is_unchanged = bool(best_name == orig_text)
            name.original_text = orig_text

        return names


@attr.s
class RemoveParenthesisPermuteLowestDistance(NameProcessor):
    """Composes ParenthesisRemover and ED-based reordering"""

    def __attrs_post_init__(self) -> None:
        self.parenthesis_remover = ParenthesisRemover(
            debug_mode=self.debug_mode
        )
        self.ed_permuter = PermuteLowestDistance(debug_mode=self.debug_mode)

    def __call__(
        self, names: List[TransliteratedName]
    ) -> List[TransliteratedName]:

        processed = self.parenthesis_remover(names)
        processed = self.ed_permuter(processed)

        return processed


@attr.s
class TransliteratedNameLoader:
    language_column: str = attr.ib(default="language")
    name_column: str = attr.ib(default="alias", repr=False)
    wikidata_id_column: str = attr.ib(default="wikidata_id", repr=False)
    conll_type_column: str = attr.ib(default="type", repr=False)
    english_column: Optional[str] = attr.ib(default="eng", repr=False)
    unicode_analyzer = UnicodeAnalyzer()
    debug_mode: bool = attr.ib(default=False)

    def load_name(self, tupl: Tuple[int, pd.Series]) -> TransliteratedName:
        row_ix, row = tupl

        if self.debug_mode:
            print(
                f"[NameLoader] Creating TransliteratedName from row {row_ix}..."
            )

        return TransliteratedName(
            text=str(row[self.name_column]),
            english_text=str(row[self.english_column]),
            language=row[self.language_column],
            unicode_analyzer=self.unicode_analyzer,
            conll_type=row[self.conll_type_column],
            wikidata_id=row[self.wikidata_id_column],
            is_unchanged=True,
        )

    def __call__(self, corpus: pd.DataFrame) -> List[TransliteratedName]:

        names = []

        for ix_row_tuple in tqdm(corpus.iterrows(), total=corpus.shape[0]):
            name = self.load_name(ix_row_tuple)
            names.append(name)

        return names


@attr.s
class Corpus:

    names: List[TransliteratedName] = attr.ib(repr=False)
    language: str = attr.ib()
    permuter_class: Type[NameProcessor] = attr.ib(
        repr=False, default=PermuteLowestDistance
    )
    num_workers: int = attr.ib(default=1, repr=False)
    out_folder: str = attr.ib(default="")
    name_column: str = attr.ib(default="alias", repr=False)
    english_column: Optional[str] = attr.ib(default="eng", repr=False)

    strip: bool = attr.ib(default=True)
    align_with_english: bool = attr.ib(default=False)
    ignore_punctuation: bool = attr.ib(default=True)
    ignore_numbers: bool = attr.ib(default=True)
    normalize_histogram: bool = attr.ib(default=True)
    preserve_fastalign_output: bool = attr.ib(repr=False, default=False)
    fastalign_verbose: bool = attr.ib(repr=False, default=False)
    permuter_debug_mode: bool = attr.ib(repr=False, default=False)
    permuter_inplace: bool = attr.ib(repr=False, default=False)
    find_best_token_permutation: bool = attr.ib(repr=False, default=False)
    analyze_unicode: bool = attr.ib(repr=False, default=True)
    debug_mode: bool = attr.ib(default=False)

    def filter_names(
        self, names: List[TransliteratedName]
    ) -> List[TransliteratedName]:
        return [n for n in names if n.text.strip() and n.english_text]

    def __attrs_post_init__(self) -> None:

        if self.debug_mode:
            print("[Corpus] Setting up Unicode analyzer...")
        self.unicode_analyzer = UnicodeAnalyzer(
            strip=self.strip,
            normalize_histogram=self.normalize_histogram,
            ignore_punctuation=self.ignore_punctuation,
            ignore_numbers=self.ignore_numbers,
        )

        if self.debug_mode:
            print("[Corpus] Setting up aligner...")
        self.fast_aligner = FastAligner(
            verbose=self.fastalign_verbose,
            preserve_raw_output=self.preserve_fastalign_output,
            debug_mode=self.debug_mode,
        )

        if self.debug_mode:
            print("[Corpus] Setting up token processor...")
        self.permuter = self.permuter_class(
            inplace=self.permuter_inplace,
            debug_mode=self.debug_mode,
            num_workers=self.num_workers,
        )

        if self.debug_mode:
            print("[Corpus] Setting up name writer...")
        self.name_writer = NameWriter(
            out_folder=self.out_folder, debug_mode=self.debug_mode
        )

        if self.find_best_token_permutation:
            if self.debug_mode:
                print("[Corpus] Applying NameProcessor...")
            self.names = self.permuter(self.names)

        if self.debug_mode:
            print("[Corpus] Filtering out names with blank text...")
        self.names = self.filter_names(self.names)

        if self.analyze_unicode:
            if self.debug_mode:
                print("[Corpus] Computing prototype...")
            self.prototype = self.unicode_analyzer.unicode_block_histogram(
                "".join(n.text for n in self.names)
            )
            self.most_common_unicode_block = self.prototype.most_common()[0][0]
        else:
            self.most_common_unicode_block = ""

        if self.align_with_english:
            if self.debug_mode:
                print("[Corpus] Aligning with English...")
            self.compute_alignments()
            self.compute_stats()

    def compute_alignments(self) -> None:

        _alignments, _names = self.fast_aligner(self.names)

        self.alignments = _alignments
        self.names = _names

    def compute_stats(self) -> None:
        self.languages: List[str] = sorted(
            list(set(name.language for name in self.names))
        )
        self.stats: Dict[str, CorpusStatistics] = {}

        if self.debug_mode:
            print("[compute_stats] Computing global corpus stats...")
        self.stats["global"] = CorpusStatistics(
            names=self.names, alignments=self.alignments
        )

        for language in tqdm(self.languages):
            if self.debug_mode:
                print(f"[compute_stats] Computing stats for {language}...")
            names_subset = [n for n in self.names if n.language == language]
            self.stats[language] = CorpusStatistics(names=names_subset)

    def split_names(
        self, with_noise_samples: bool = False
    ) -> Dict[str, List[TransliteratedName]]:
        split: Dict[str, List[TransliteratedName]] = {
            "anomalous": [],
            "non_anomalous": [],
        }

        for name in self.names:
            if not with_noise_samples and name.noise_sample:
                continue
            tag = "anomalous" if name.anomalous else "non_anomalous"
            split[tag].append(name)

        return split

    def write_anomaly_info(self, write_noise_samples=False):
        self.name_writer.write(
            self.split_names(write_noise_samples),
            unicode_block_mode=True,
            write_permutations_mode=False,
        )

    def write_permutations(self):
        self.name_writer.write(
            {"all_names": self.names},
            unicode_block_mode=False,
            write_permutations_mode=True,
        )

    def add_words(self, additional_names: List[TransliteratedName]):
        self.names.extend(additional_names)


class AnomalousTagger:
    def classify(self, name: TransliteratedName) -> Optional[bool]:
        raise NotImplementedError

    def __call__(
        self, names: Iterable[TransliteratedName]
    ) -> Iterable[TransliteratedName]:
        return [
            TransliteratedName(
                text=n.text,
                unicode_analyzer=n.unicode_analyzer,
                anomalous=self.classify(n),
                language=n.language,
                is_unchanged=n.is_unchanged,
            )

            for n in names
        ]


@attr.s
class IncorrectBlockTagger(AnomalousTagger):
    """Tags a name as anomalous if its most common Unicode block is incorrect"""

    expected_block: str = attr.ib()

    def classify(self, name: TransliteratedName) -> Optional[bool]:
        return name.most_common_unicode_block != self.expected_block


@attr.s
class MissingBlockTagger(AnomalousTagger):
    """Tags a name as anomalous if it has no characters from given Unicode block"""

    missing_block: str = attr.ib()

    def classify(self, name: TransliteratedName) -> Optional[bool]:
        return self.missing_block not in name.unicode_block_histogram


@attr.s
class JSDTagger(AnomalousTagger):
    """Tags a name as anomalous if its distribution of Unicode blocks is
    sufficiently far from the per-language Unicode block distribution as
    measured by the Jensen-Shannon divergence."""

    per_language_distribution: Dict[str, float] = attr.ib()
    critical_value: float = attr.ib(default=0.1)
    distance_measure: str = attr.ib(default="jensen_shannon")

    def distance(
        self,
        p: Dict[str, float],
        q: Dict[str, float],
    ) -> float:
        """Computes distance between PMFs p and q using dictances library"""

        return {
            "jensen_shannon": dt.jensen_shannon,
            "kullback_leibler": dt.kullback_leibler,
        }[self.distance_measure](p, q)

    def classify(self, name: TransliteratedName) -> bool:
        observed_distance = self.distance(
            name.unicode_block_histogram, self.per_language_distribution
        )

        return observed_distance >= self.critical_value


class HiraganaKatakanaTagger(AnomalousTagger):
    """Tags names as anomalous/non-anomalous based on their Hiragana/Katakana characters.

    Analyzes Japanese, Modern Chinese variants and Classical Chinese.

    In case of Japanese, the name is tagged as anomalous if it does not include Katakana or Hiragana.
    In case of Chinese, the name is anomalous if it contains Katakana/Hiragana
    In case another language is encountered, the tagger abstains.
    """

    def classify(self, name: TransliteratedName) -> Optional[bool]:
        hist = name.unicode_block_histogram

        # match chinese variants/japanese/classical chinese with regex
        re_chinese_japanese = re.compile(r"^(ja|zh-*|lzh|wuu)")

        # all other languages should abstain

        if not re_chinese_japanese.match(name.language):
            return None

        contains_kana = "HIRAGANA" in hist or "KATAKANA" in hist

        return contains_kana if name.language != "ja" else not contains_kana


class CJKTagger(AnomalousTagger):
    """Tags names as anomalous/non-anomalous based on their Hiragana/Katakana characters.

    Analyzes Japanese, Modern Chinese variants and Classical Chinese.

    Words are anomalous if they do not contain any CJK.
    """

    def classify(self, name: TransliteratedName) -> Optional[bool]:
        hist = name.unicode_block_histogram

        # match chinese variants/japanese/classical chinese with regex
        re_chinese_japanese = re.compile(r"^(ja|zh-*|lzh|wuu)")

        # all other languages should abstain

        if not re_chinese_japanese.match(name.language):
            return None

        contains_cjk = any(block.startswith("CJK") for block in hist)

        return contains_cjk