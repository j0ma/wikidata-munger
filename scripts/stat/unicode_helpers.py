from functools import lru_cache
from collections import Counter
from typing import Generator, Set, List, Union, Dict, Any, IO, Tuple, Callable
import unicodedata as ud
from unicodeblock import blocks

CACHE_MAX_SIZE = 10000


class UnicodeAnalyzer:
    def __init__(
        self,
        strip: bool = False,
        ignore_punctuation: bool = False,
        normalize_histogram: bool = True,
    ) -> None:
        self.strip = strip
        self.ignore_punctuation = ignore_punctuation
        self.normalize_histogram = normalize_histogram

    def is_punctuation(self, s: str) -> bool:
        is_punc = ud.category(s).startswith("P")
        is_symb = ud.category(s).startswith("S")

        return is_punc or is_symb

    def maybe_strip(self, word: str) -> str:
        return str(word).strip() if self.strip else str(word)

    @lru_cache(maxsize=CACHE_MAX_SIZE)
    def unicode_blocks(self, word: str) -> Counter:
        punctuation_cond = (
            lambda w: not self.is_punctuation(str(w))

            if self.ignore_punctuation
            else True
        )

        return Counter(
            blocks.of(c)

            for c in self.maybe_strip(word)

            if blocks.of(c) and punctuation_cond(c)
        )

    @lru_cache(maxsize=CACHE_MAX_SIZE)
    def most_common_unicode_block(self, word: str) -> str:
        return self.unicode_blocks(word).most_common(1)[0][0]

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
