from functools import lru_cache
from collections import Counter
from typing import Generator, Set, List, Union, Dict, Any, IO, Tuple
import unicodedata as ud
from unicodeblock import blocks


@lru_cache(maxsize=None)
def unicode_blocks(word: str) -> Counter:
    return Counter(
        blocks.of(c) for c in str(word)
    )  # wrap in str() to handle e.g. digits


@lru_cache(maxsize=None)
def most_common_unicode_block(word: str) -> str:
    return unicode_blocks(word).most_common(1)[0][0]
