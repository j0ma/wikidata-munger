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
    Callable,
    Iterable,
    Optional,
)
from pathlib import Path
import tempfile as tf
import subprocess
import re
import io

import unicodedata as ud
from unicodeblock import blocks
import pandas as pd
import numpy as np
import attr

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
        self.normalize_histogram = (normalize_histogram,)
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
class Word:
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
        return hash(self.word + self.language)


class Alignment:
    def __init__(
        self,
        alignment_str: Optional[str] = None,
        src: Optional[str] = None,
        tgt: Optional[str] = None,
        word: Optional[Word] = None,
    ) -> None:

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

            # TODO: remove try-except
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
        # return f"Alignment({self.alignment_str})"

        return f"Alignment({self.word.text}->{self.word.english_text})"


@attr.s
class AlignmentCollection:

    alignments: Iterable[Alignment] = attr.ib()
    mce: Optional[float] = attr.ib(default=None, repr=False)
    tce: Optional[int] = attr.ib(default=None, repr=False)

    def __len__(self):
        return len(self.alignments)

    def __iter__(self):
        return self.alignments.__iter__()

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
        cls, alignment_file: str, words: Optional[Iterable[str]]
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


class Tagger:
    def __call__(self, words: Iterable[Word]) -> Iterable[Word]:
        raise NotImplementedError

    def classify(self, word: Word) -> Optional[bool]:
        raise NotImplementedError

    def __call__(self, words: Iterable[Word]) -> Iterable[Word]:
        return [
            Word(
                text=w.text,
                unicode_analyzer=w.unicode_analyzer,
                anomalous=self.classify(w),
                language=w.language,
            )

            for w in words
        ]


@attr.s
class IncorrectBlockTagger(Tagger):
    """Tags a word as anomalous if its most common Unicode block is incorrect"""

    expected_block: str = attr.ib()

    def classify(self, word: Word) -> Optional[bool]:
        return word.most_common_unicode_block != self.expected_block


@attr.s
class MissingBlockTagger(Tagger):
    """Tags a word as anomalous if it has no characters from given Unicode block"""

    missing_block: str = attr.ib()

    def classify(self, word: Word) -> Optional[bool]:
        return self.missing_block not in word.unicode_block_histogram


@attr.s
class JSDTagger(Tagger):
    """Tags a word as anomalous if its distribution of Unicode blocks is
    sufficiently far from the per-language Unicode block distribution as
    measured by the Jensen-Shannon divergence."""

    per_language_distribution: Dict[str, Dict[str, float]] = attr.ib()
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
        }.get(self.distance_measure)(p, q)

    def classify(self, word: Word) -> bool:
        observed_distance = self.distance(
            word.unicode_block_histogram, self.per_language_distribution
        )

        return observed_distance >= self.critical_value


class HiraganaKatakanaTagger(Tagger):
    """Tags words as anomalous/non-anomalous based on their Hiragana/Katakana characters.

    Analyzes Japanese, Modern Chinese variants and Classical Chinese.

    In case of Japanese, the word is tagged as anomalous if it does not include Katakana or Hiragana.
    In case of Chinese, the word is anomalous if it contains Katakana/Hiragana
    In case another language is encountered, the tagger abstains.
    """

    def classify(self, word: Word) -> Optional[bool]:
        hist = word.unicode_block_histogram

        # match chinese variants/japanese/classical chinese with regex
        re_chinese_japanese = re.compile(r"^(ja|zh-*|lzh|wuu)")

        # all other languages should abstain

        if not re_chinese_japanese.match(word.language):
            return None

        contains_kana = "HIRAGANA" in hist or "KATAKANA" in hist

        return contains_kana if word.language != "ja" else not contains_kana


class CJKTagger(Tagger):
    """Tags words as anomalous/non-anomalous based on their Hiragana/Katakana characters.

    Analyzes Japanese, Modern Chinese variants and Classical Chinese.

    Words are anomalous if they do not contain any CJK.
    """

    def classify(self, word: Word) -> Optional[bool]:
        hist = word.unicode_block_histogram

        # match chinese variants/japanese/classical chinese with regex
        re_chinese_japanese = re.compile(r"^(ja|zh-*|lzh|wuu)")

        # all other languages should abstain

        if not re_chinese_japanese.match(word.language):
            return None

        contains_cjk = any(block.startswith("CJK") for block in hist)

        return contains_cjk


@attr.s
class Corpus:

    corpus_df: pd.DataFrame = attr.ib(repr=False)
    language: str = attr.ib()
    out_folder: str = attr.ib(default="")
    word_column: str = attr.ib(default="alias", repr=False)
    english_column: Optional[str] = attr.ib(default="eng", repr=False)

    strip: bool = attr.ib(default=True)
    align_with_english: bool = attr.ib(default=False)
    ignore_punctuation: bool = attr.ib(default=True)
    ignore_numbers: bool = attr.ib(default=True)
    normalize_histogram: bool = attr.ib(default=True)

    def __attrs_post_init__(self) -> None:
        self.unicode_analyzer = UnicodeAnalyzer(
            strip=self.strip,
            normalize_histogram=self.normalize_histogram,
            ignore_punctuation=self.ignore_punctuation,
            ignore_numbers=self.ignore_numbers,
        )
        self.words = self.corpus_df.apply(  # [self.word_column]
            lambda row: Word(
                text=str(row[self.word_column]),
                english_text=str(row[self.english_column]),
                language=self.language,
                unicode_analyzer=self.unicode_analyzer,
            ),
            axis=1,
        ).tolist()

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
        alignment_train_data = "/tmp/fastalign_train"  # tf.mktemp()
        fastalign_output = "/tmp/fastalign_output"  # tf.mktemp()

        print(f"Alignment training data going to: {alignment_train_data}")
        print(f"Fast-align output going to: {fastalign_output}")

        # first we write our words into a temporary file for fast_align
        with open(alignment_train_data, "w") as f_out:
            for word in self.words:

                word_text = word.text.replace(" ", "▁")
                english_text = word.english_text.replace(" ", "▁")

                if word.english_text:
                    f_out.write(
                        f"{' '.join(word_text)} ||| {' '.join(english_text)}\n"
                    )

        # then perform the actual call to fast_align
        try:
            with open(fastalign_output, "w", encoding="utf-8") as f_out:
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
                )

        except subprocess.CalledProcessError as err:
            print(err)
        except OSError as err:
            print(err)
            raise ValueError("fast_align must be installed!")

        self.alignments = AlignmentCollection.from_alignment_file(
            fastalign_output, words=self.words
        )

        assert len(self.alignments) == len(
            self.words
        ), f"(alignments) {len(self.alignments)} != (words) {len(self.words)}"

    def split_words(
        self, with_noise_samples: bool = False
    ) -> Dict[str, Iterable[Word]]:
        split = {"anomalous": [], "non_anomalous": []}

        for word in self.words:
            if not with_noise_samples and word.noise_sample:
                continue
            tag = "anomalous" if word.anomalous else "non_anomalous"
            split[tag].append(word)

        return split

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

    def write_to_folder(self, write_noise_samples=False):
        if not self.out_folder:
            self.out_folder = Path(tf.mkdtemp())
        else:
            self.out_folder = Path(self.out_folder)

            if not self.out_folder.exists():
                self.out_folder.mkdir()

        split = self.split_words(with_noise_samples=write_noise_samples)
        anomalous, non_anomalous = split["anomalous"], split["non_anomalous"]

        anomalous_path = self.out_folder / f"{self.language}"

        if not anomalous_path.exists():
            anomalous_path.mkdir()

        anomalous_path = anomalous_path / f"{self.language}_anomalous.txt"

        non_anomalous_path = self.out_folder / f"{self.language}"

        if not non_anomalous_path.exists():
            non_anomalous_path.mkdir()
        non_anomalous_path = (
            non_anomalous_path / f"{self.language}_non_anomalous.txt"
        )

        with open(anomalous_path, "w", encoding="utf-8") as f_anom:
            for w in sorted(anomalous, key=lambda w: w.text):
                f_anom.write(f"{w.text}\t{w.most_common_unicode_block}\n")

        with open(non_anomalous_path, "w", encoding="utf-8") as f_non_anom:
            for w in sorted(non_anomalous, key=lambda w: w.text):
                f_non_anom.write(f"{w.text}\t{w.most_common_unicode_block}\n")

        print(
            f"[{self.language}] Anomalous/non-anomalous words written to {self.out_folder}"
        )

    def add_words(self, words: Iterable[Word]) -> None:
        self.words.extend(words)
