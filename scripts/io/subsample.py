#!/usr/bin/env python

"""
Subsamples the input data to create a reasonable sized and less imbalanced training/dev/test set.
"""

from collections import Counter
from typing import Optional

import wikidata_helpers as wh
import pandas as pd
import numpy as np
import click
import attr

SUPPORTED_SAMPLERS = ["uniform", "empirical", "exponential_smoothing"]
SUPPORTED_FORMATS = ["tsv", "jsonl"]

DEFAULT_SEED = 1917


@attr.s
class Sampler:
    """General class representing a sampler that samples documents according
    to the process of first sampling a language and then a document in the given language."""

    random_seed: int = attr.ib(default=DEFAULT_SEED)


@attr.s
class EmpiricalLanguageDistributionSampler(Sampler):
    """Uniformly samples rows of pandas DataFrame, effectively drawing
    samples according to the empirical language distribution of the data set.

    Returns another DataFrame."""

    def __call__(self, df: pd.DataFrame, n: Optional[int]) -> pd.DataFrame:

        np.random.set_state(self.random_seed)

        n_rows, _ = df.shape

        if not n:
            n = n_rows

        mask = bool(np.random.rand(n_rows) > 0.5)

        return df[mask].reset_index(drop=True).head(n)


@attr.s
class UniformLanguageDistributionSampler(Sampler):
    """Uniformly samples languages from the corpus and then rows from each language.

    Returns another DataFrame."""

    language_column: Optional[str] = attr.ib(default="language")

    def __call__(
        self,
        df: pd.DataFrame,
        n: Optional[int],
        language_column: Optional[str],
    ) -> pd.DataFrame:

        if not n:
            return df

        # sample a random number of names to draw from each language
        lc = language_column or self.language_column
        n_samples_per_language: Counter = Counter(df[lc].sample(n))

        # sample the prescribed number of names for each language
        sampled_rows = pd.concat(
            [
                df[df[lc] == lang].sample(
                    n_samples, random_state=self.random_seed
                )

                for lang, n_samples in n_samples_per_language.items()
            ],
            ignore_index=True,
        )

        return sampled_rows


class ExponentialSmoothingLanguageDistributionSampler(Sampler):
    """Re-weights the language distribution with exponential smoothing
    similar to what was done with multilingual BERT, and draws samples
    from the re-weighted language distribution.

    Returns another DataFrame."""

    language_column: Optional[str] = attr.ib(default="language")
    smoothing_factor: float = attr.ib(default=0.7)

    def __call__(
        self,
        df: pd.DataFrame,
        n: Optional[int],
        language_column: Optional[str],
    ) -> pd.DataFrame:

        if not n:
            return df

        # sample a random number of names to draw from each language
        lc = language_column or self.language_column
        language_distribution = df[lc].value_counts(normalize=True)
        smoothed_language_weights = (
            self.smoothing_factor * language_distribution
        )

        weights_for_sampling = df[lc].apply(
            lambda l: smoothed_language_weights[lc]
        )

        sampled_rows = df.sample(
            weights=weights_for_sampling, random_state=self.random_seed
        )

        return sampled_rows


@click.command()
@click.option("--input-file", "-i", required=True)
@click.option("--output-file", "-o", required=True)
@click.option("--io-format", "-f", type=click.Choice(SUPPORTED_FORMATS))
@click.option("--chunksize", "-c", type=int, default=-1)
@click.option("--sampler", "-s", type=click.Choice(SUPPORTED_SAMPLERS))
@click.option("--debug-mode", "-d", is_flag=True)
@click.option("--random-seed", "-r", type=int, default=DEFAULT_SEED)
def main(input_file, output_file, io_format, chunksize, sampler, debug_mode):

    data = wh.read(
        input_file, io_format, chunksize=chunksize if chunksize > 0 else None
    )

    if chunksize > 0 and debug_mode:
        data = next(data)
    elif chunksize > 0 and not debug_mode:
        data = pd.concat(chunk for chunk in data)

    subsampler = {"empirical": EmpiricalLanguageDistributionSampler}.get(
        sampler
    )(random_seed)

    sub = subsampler(data)

    print(sub)


if __name__ == "__main__":
    main()
