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
import re
import io
import os

import unicodedata as ud
from unicodeblock import blocks
import pandas as pd
import numpy as np
import attr

import editdistance

import dictances as dt

CACHE_MAX_SIZE = 10000


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

    @lru_cache(maxsize=CACHE_MAX_SIZE)
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

    @lru_cache(maxsize=CACHE_MAX_SIZE)
    def most_common_unicode_block(self, word: str) -> str:
        try:
            return self.unicode_blocks(word).most_common(1)[0][0]
        except IndexError:
            return ""

    @lru_cache(maxsize=CACHE_MAX_SIZE)
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


@attr.s
class TransliteratedName:
    text: str = attr.ib()
    language: str = attr.ib()
    unicode_analyzer: UnicodeAnalyzer = attr.ib(repr=False)
    anomalous: Optional[bool] = attr.ib(default=None)
    noise_sample: Optional[bool] = attr.ib(default=False)
    english_text: Optional[str] = attr.ib(default=None)

    @property
    def most_common_unicode_block(self) -> str:
        return self.unicode_analyzer.most_common_unicode_block(self.text)

    @property
    def unicode_block_histogram(self) -> Dict[str, float]:
        return self.unicode_analyzer.unicode_block_histogram(self.text)

    def __hash__(self) -> int:
        return hash(self.text + self.language)

    def add_alignment(self, alignment) -> None:
        self.alignment = alignment


class Alignment:
    def __init__(
        self,
        alignment_str: Optional[str] = None,
        src: Optional[str] = None,
        tgt: Optional[str] = None,
        word: Optional[TransliteratedName] = None,
    ) -> None:

        if alignment_str:
            alignment_str = alignment_str.strip()

        self.alignment_str = alignment_str
        self.word = word

        src_tgt_not_none = src or tgt

        self.cross_alignments = set([])

        if not alignment_str and not src_tgt_not_none:
            print(
                "Must either give an alignment string or a (src_ix, tgt_ix) pair!"
            )

            if word:
                print(f"Word: {word}")

            return

        max_tgt_seen = 0

        for at in alignment_str.split(" "):

            try:
                # src, tgt = [int(x) for x in at.split("-")]
                tgt, src = [int(x) for x in at.split("-")]

                if tgt < max_tgt_seen:
                    self.cross_alignments.add(f"{src}->{tgt}")

                max_tgt_seen = max(max_tgt_seen, tgt)
            except:
                continue

    @property
    def n_cross_alignments(self) -> int:
        return len(self.cross_alignments)

    def __repr__(self) -> str:
        return f"Alignment({self.word.text}->{self.word.english_text})"


@attr.s
class NameWriter:

    out_folder: Union[str, Path] = attr.ib(default="")
    verbose: bool = attr.ib(default=False)

    def write(self, split: Dict[str, Iterable[TransliteratedName]]) -> None:
        if not self.out_folder:
            self.out_folder = Path(tf.mkdtemp())
        else:
            if self.verbose:
                print(
                    f"Output folder {self.out_folder} not found. Creating..."
                )
            self.out_folder = Path(self.out_folder)

            if not self.out_folder.exists():
                self.out_folder.mkdir(parents=True)

        for c, names in split.items():
            path = self.out_folder / f"{c}.txt"

            with open(path, "w", encoding="utf-8") as f:
                for name in sorted(names, key=lambda name: name.text):
                    f.write(f"{name.text}\t{name.most_common_unicode_block}\n")

            if self.verbose:
                print(f"[{c}] Names written to {path}")


class CorpusStatistics:
    def __init__(
        self,
        names: Iterable[TransliteratedName],
        alignments: Optional[Iterable[Alignment]] = None,
    ) -> None:
        """Cross-alignment statistics for a transliterated name corpus

        Notes
        -----
        - if alignments is None, names is assumed to contain alignments
        - if names doesn't, then alignments must be passed
        """

        self.mce: Optional[float] = None
        self.tce: Optional[int] = None

        self.names = names
        self.alignments = (
            alignments if alignments else (n.alignment for n in names)
        )

    @property
    def mean_cross_alignments(self) -> float:
        if not self.mce:
            self.mce = np.mean([a.n_cross_alignments for a in self.alignments])

        return self.mce

    @property
    def total_cross_alignments(self) -> float:
        if not self.tce:
            self.tce = sum([a.n_cross_alignments for a in self.alignments])

        return self.tce


@attr.s
class AlignmentCollection:

    alignments: Iterable[Alignment] = attr.ib()
    mce: Optional[float] = attr.ib(default=None, repr=False)
    tce: Optional[int] = attr.ib(default=None, repr=False)

    def __len__(self):
        return len(self.alignments)

    def __iter__(self):
        return self.alignments.__iter__()

    # TODO: deprecate these in favor of CorpusStatistics
    @property
    def mean_cross_alignments(self) -> float:
        if not self.mce:
            self.mce = np.mean([a.n_cross_alignments for a in self.alignments])

        return self.mce

    @property
    def total_cross_alignments(self) -> float:
        if not self.tce:
            self.tce = sum([a.n_cross_alignments for a in self.alignments])

        return self.tce

    @classmethod
    def from_alignment_file(
        cls, alignment_file: str, words: Optional[Iterable[TransliteratedName]]
    ):
        with open(alignment_file, "r", encoding="utf-8") as f:

            if words:
                alignments = [
                    Alignment(alignment_str=alignment_string, word=word)

                    for alignment_string, word in zip(f, words)
                ]

            else:
                alignments = [
                    Alignment(alignment_str=alignment_string)

                    for alignment_string in f
                ]

            return cls(alignments)


@attr.s
class UniversalRomanizer:
    """Python binding to ISI's Universal Romanizer written in Perl

    Notes
    -----
    - By default, assumes that the uroman invocation command is stored
      in the UROMAN_CMD environment variable. It is also possible to pass
      the command as an argument to __init__.
    """

    uroman_cmd: Optional[str] = attr.ib(default=os.environ["UROMAN_CMD"])

    def __call__(self, strings: Iterable[str]) -> Iterable[str]:

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

                for line in completed_pid.stdout.split("\n")

                if line.strip()
            ]

            assert len(uroman_output) == len(strings)

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

    def __call__(
        self, names: Iterable[TransliteratedName]
    ) -> Tuple[AlignmentCollection, Iterable[TransliteratedName]]:

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

                name_text = name.text.replace(" ", "▁")
                english_text = name.english_text.replace(" ", "▁")

                if name.english_text:
                    f_out.write(
                        f"{' '.join(name_text)} ||| {' '.join(english_text)}\n"
                    )

        # then perform the actual call to fast_align
        try:
            with open(fastalign_output, "w", encoding="utf-8") as f_out, open(
                "/dev/null", "w"
            ) as null:
                subprocess.check_call(
                    [
                        "fast_align",
                        "-v",
                        "-d",
                        "-o",
                        "-i",
                        alignment_train_data,
                    ],
                    stdout=f_out,
                    stderr=null,
                )

        except subprocess.CalledProcessError as err:
            print(err)
        except OSError as err:
            print(err)
            raise ValueError("fast_align must be installed!")

        # get all the alignments out as a collection
        alignments = AlignmentCollection.from_alignment_file(
            fastalign_output, words=names
        )

        # then link the names with the alignments

        for name, alignment in zip(names, alignments):
            name.add_alignment(alignment)

        # make sure we have as many alignments as we have names
        assert len(alignments) == len(names)

        # finally remove the temporary files unless told otherwise

        if not self.preserve_raw_output:
            alignment_train_data.unlink()
            fastalign_output.unlink()

        return alignments, names


@attr.s
class Permuter:

    inplace: bool = attr.ib(default=False)
    debug_mode: bool = attr.ib(default=False)

    def __call__(
        self, names: Iterable[TransliteratedName]
    ) -> Iterable[TransliteratedName]:
        if self.inplace:
            return self._call_inplace(names)
        else:
            return self._call_immutable(names)

    def _call_immutable(
        self, names: Iterable[TransliteratedName]
    ) -> Iterable[TransliteratedName]:
        raise NotImplementedError

    def _call_inplace(
        self, names: Iterable[TransliteratedName]
    ) -> Iterable[TransliteratedName]:
        raise NotImplementedError


@attr.s
class FirstCommaBasedPermuter(Permuter):
    """Permutes tokens around the first comma (,) if one is present.

    Meant to catch cases like 'Biden, Joe' => 'Joe Biden'
    """

    comma: str = attr.ib(default=",")

    def permute(self, name_text: str) -> str:
        comma_ix = name_text.find(self.comma)

        if comma_ix == -1:
            return name_text
        else:
            head = name_text[:comma_ix]
            tail = name_text[(comma_ix + 1) :]

            return f"{tail} {head}".strip()

    def _call_immutable(
        self, names: Iterable[TransliteratedName]
    ) -> Iterable[TransliteratedName]:
        output = []

        for name in names:
            if self.debug_mode:
                print(f"[{name.english_text}] {name.text}".strip())
            permuted_name = self.permute(name.text)

            if self.debug_mode:
                print(f"[{name.english_text}] {permuted_name}".strip())
            output.append(
                TransliteratedName(
                    text=permuted_name,
                    language=name.language,
                    unicode_analyzer=name.unicode_analyzer,
                    anomalous=name.anomalous,
                    noise_sample=name.noise_sample,
                    english_text=name.english_text,
                )
            )

        return output

    def _call_inplace(
        self, names: Iterable[TransliteratedName]
    ) -> Iterable[TransliteratedName]:
        for name in names:
            if self.debug_mode:
                print(f"[{name.english_text}] {name.text}".strip())
            permuted_name = self.permute(name.text)

            if self.debug_mode:
                print(f"[{name.english_text}] {permuted_name}".strip())
            name.text = permuted_name

        return names


@attr.s
class LowestDistancePermuter(Permuter):
    """Permutes tokens in a name to achieve the lowest distance
    between the name and its romanized version.
    """

    distance_function: Callable[[str, str], float] = attr.ib(
        default=editdistance.eval
    )

    romanizer: UniversalRomanizer = UniversalRomanizer()

    length_lower_bound: int = attr.ib(repr=False, default=2)
    length_upper_bound: int = attr.ib(repr=False, default=4)

    def _call_immutable(
        self, names: Iterable[TransliteratedName]
    ) -> Iterable[TransliteratedName]:

        output = []

        romanized_names = self.romanizer([n.text for n in names])

        for name, romanized_name in zip(names, romanized_names):
            tokens = name.text.split()
            romanized_tokens = romanized_name.split()

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
                        best_name = permutation

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
                    )
                )

        return output

    def _call_inplace(
        self, names: Iterable[TransliteratedName]
    ) -> Iterable[TransliteratedName]:

        romanized_names = self.romanizer([n.text for n in names])

        for name, romanized_name in zip(names, romanized_names):
            tokens = name.text.split()
            romanized_tokens = romanized_name.split()

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
                name_text = " ".join(permutation)
                rom_name_text = " ".join(rom_permutation)
                ed = self.distance_function(rom_name_text, name.english_text)

                if ed < best_distance:
                    best_distance = ed
                    best_name = name_text

            name.text = best_name

        return names


@attr.s
class Corpus:

    corpus_df: pd.DataFrame = attr.ib(repr=False)
    language: str = attr.ib()
    permuter_class: Type[Permuter] = attr.ib(
        repr=False, default=LowestDistancePermuter
    )
    out_folder: str = attr.ib(default="")
    word_column: str = attr.ib(default="alias", repr=False)
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

    def __attrs_post_init__(self) -> None:
        self.unicode_analyzer = UnicodeAnalyzer(
            strip=self.strip,
            normalize_histogram=self.normalize_histogram,
            ignore_punctuation=self.ignore_punctuation,
            ignore_numbers=self.ignore_numbers,
        )

        self.fast_aligner = FastAligner(
            verbose=self.fastalign_verbose,
            preserve_raw_output=self.preserve_fastalign_output,
        )

        self.permuter = self.permuter_class(
            inplace=self.permuter_inplace, debug_mode=self.permuter_debug_mode
        )

        self.name_writer = NameWriter(out_folder=self.out_folder)

        self.words = self.corpus_df.apply(
            lambda row: TransliteratedName(
                text=str(row[self.word_column]),
                english_text=str(row[self.english_column]),
                language=self.language,
                unicode_analyzer=self.unicode_analyzer,
            ),
            axis=1,
        ).tolist()

        if self.find_best_token_permutation:
            self.words = self.permuter(self.words)

        self.prototype = self.unicode_analyzer.unicode_block_histogram(
            "".join(w.text for w in self.words)
        )

        if self.prototype:
            self.most_common_unicode_block = self.prototype.most_common()[0][0]
        else:
            self.most_common_unicode_block = ""

        if self.align_with_english:
            self.compute_alignments()

    def compute_alignments(self) -> None:

        _alignments, _words = self.fast_aligner(self.words)

        self.alignments = _alignments
        self.words = _words

    def split_words(
        self, with_noise_samples: bool = False
    ) -> Dict[str, List[TransliteratedName]]:
        split: Dict[str, List[TransliteratedName]] = {
            "anomalous": [],
            "non_anomalous": [],
        }

        for word in self.words:
            if not with_noise_samples and word.noise_sample:
                continue
            tag = "anomalous" if word.anomalous else "non_anomalous"
            split[tag].append(word)

        return split

    def write_to_folder(self, write_noise_samples=False):
        self.name_writer.write(self.split_words(write_noise_samples))

    @property
    def mean_cross_alignments(self) -> Optional[float]:
        if not self.alignments:
            return None

        return self.alignments.mean_cross_alignments

    @property
    def total_cross_alignments(self) -> Optional[float]:
        if not self.alignments:
            return None

        return self.alignments.total_cross_alignments


class Tagger:
    def classify(self, name: TransliteratedName) -> Optional[bool]:
        raise NotImplementedError

    def __call__(
        self, names: Iterable[TransliteratedName]
    ) -> Iterable[TransliteratedName]:
        return [
            TransliteratedName(
                text=w.text,
                unicode_analyzer=w.unicode_analyzer,
                anomalous=self.classify(w),
                language=w.language,
            )

            for w in names
        ]


@attr.s
class IncorrectBlockTagger(Tagger):
    """Tags a name as anomalous if its most common Unicode block is incorrect"""

    expected_block: str = attr.ib()

    def classify(self, name: TransliteratedName) -> Optional[bool]:
        return name.most_common_unicode_block != self.expected_block


@attr.s
class MissingBlockTagger(Tagger):
    """Tags a name as anomalous if it has no characters from given Unicode block"""

    missing_block: str = attr.ib()

    def classify(self, name: TransliteratedName) -> Optional[bool]:
        return self.missing_block not in name.unicode_block_histogram


@attr.s
class JSDTagger(Tagger):
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


class HiraganaKatakanaTagger(Tagger):
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


class CJKTagger(Tagger):
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
