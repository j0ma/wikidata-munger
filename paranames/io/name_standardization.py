#!/usr/bin/env python

from typing import Dict, Tuple, Generator
import itertools as it
from pathlib import Path
import tempfile

import click
import numpy as np
import pandas as pd
from tqdm import tqdm
from sklearn.metrics import classification_report
from flyingsquid.label_model import LabelModel
import paranames.io.wikidata_helpers as wh
import paranames.analysis.script_analysis as sa
from p_tqdm import p_map
import orjson


def standardize_names(
    data: pd.DataFrame,
    language_column: str,
    alias_column: str,
    id_column: str,
    num_workers: int,
    chunksize: int,
    human_readable_lang_names: Dict[str, str],
    permuter_type: str,
    corpus_stats_output: str,
    debug_mode: bool = False,
    chunk_rows: bool = False,
    corpus_require_english: bool = False,
    corpus_filter_blank: bool = False,
    *args,
    **kwargs,
) -> pd.DataFrame:

    permuter_class = {
        "comma": sa.PermuteFirstComma,
        "edit_distance": sa.PermuteLowestDistance,
        "remove_parenthesis_permute_comma": sa.RemoveParenthesisPermuteComma,
        "remove_parenthesis_edit_distance": sa.RemoveParenthesisPermuteLowestDistance,
        "remove_parenthesis": sa.ParenthesisRemover,
    }[permuter_type]

    num_rows, num_columns = data.shape

    if chunk_rows:
        corpus_chunks = (chunk for chunk in np.array_split(data, chunksize))
    else:
        corpus_chunks = (chunk for chunk in (data,))

    name_loader = sa.TransliteratedNameLoader(
        language_column=language_column,
        wikidata_id_column=id_column,
        debug_mode=False,
    )
    names = list(
        it.chain.from_iterable(
            p_map(name_loader, corpus_chunks, num_cpus=num_workers)
        )
    )

    should_compute_stats = bool("edit_distance" in permuter_type)
    print("[standardize_names] Creating pooled corpus...")
    pooled_corpus = sa.Corpus(
        names=names,
        language="all",
        permuter_class=permuter_class,
        debug_mode=debug_mode,
        normalize_histogram=True,
        ignore_punctuation=True,
        ignore_numbers=False,
        align_with_english=True,
        fastalign_verbose=True,
        permuter_inplace=True,
        find_best_token_permutation=True,
        analyze_unicode=False,
        preserve_fastalign_output=False,
        require_english=False,
        filter_out_blank=False,
    )

    print(f"[standardize_names] Computing corpus statistics...")
    with open(corpus_stats_output, "w", encoding="utf8") as f_stats:
        for lang, stats_per_lang in pooled_corpus.stats.items():
            lang_long = human_readable_lang_names.get(lang, lang)
            avg_alignments = stats_per_lang.mean_cross_alignments
            total_permuted = stats_per_lang.total_permuted
            total_surviving = stats_per_lang.total_surviving
            print(
                f"{lang_long}\t{avg_alignments}\t{total_permuted}\t{total_surviving}",
                file=f_stats,
            )

    print(f"[standardize_names] Replacing old names...")
    data[alias_column] = [n.text for n in pooled_corpus.names]
    data["unchanged"] = [n.is_unchanged for n in pooled_corpus.names]

    # finally mask out rows with now empty labels
    labels_nonempty = data[alias_column].apply(lambda s: bool(s))
    data = data[labels_nonempty]

    return data


@click.command()
@click.option("--input-file", "-i")
@click.option("--output-file", "-o")
@click.option("--io-format", "-f", default="tsv")
@click.option("--id-column", "-id", default="wikidata_id")
@click.option("--type-column", "-t", default="type")
@click.option("--alias-column", "-a", default="alias")
@click.option("--english-column", "-e", default="eng")
@click.option("--language-column", "-l", default="language")
@click.option("--num-workers", type=int, default=2)
@click.option("--chunksize", type=int, default=15000)
@click.option("--human-readable-langs-path", required=True)
@click.option(
    "--permuter-type", required=True, type=click.Choice(sa.permuter_types)
)
@click.option("--corpus-require-english", is_flag=True, help="deprecated")
@click.option("--corpus-filter-blank", is_flag=True, help="deprecated")
@click.option("--debug-mode", is_flag=True)
@click.option("--corpus-stats-output", required=True)
@click.option("--chunk_rows", is_flag=True)
def main(
    input_file,
    output_file,
    io_format,
    id_column,
    type_column,
    alias_column,
    english_column,
    language_column,
    num_workers,
    chunksize,
    human_readable_langs_path,
    permuter_type,
    corpus_require_english,
    corpus_filter_blank,
    debug_mode,
    corpus_stats_output,
    chunk_rows,
):

    # read in human readable language names
    with open(human_readable_langs_path, encoding="utf8") as f:
        human_readable_lang_names = orjson.loads(f.read())

    # read in data and sort it by language
    if debug_mode:
        print("Reading data...")
    data = wh.read(input_file, io_format=io_format)

    # need to sort by language to ensure ordered chunks
    if debug_mode:
        print("Sorting by language column...")
    data = data.sort_values(language_column)

    # standardize names
    data = standardize_names(
        data,
        id_column=id_column,
        type_column=type_column,
        num_workers=num_workers,
        chunksize=chunksize,
        alias_column=alias_column,
        permuter_type=permuter_type,
        language_column=language_column,
        human_readable_lang_names=human_readable_lang_names,
        corpus_require_english=corpus_require_english,
        corpus_filter_blank=corpus_filter_blank,
        debug_mode=debug_mode,
        corpus_stats_output=corpus_stats_output,
        chunk_rows=chunk_rows,
    )

    # write to disk
    wh.write(data, output_file, io_format=io_format)


if __name__ == "__main__":
    main()
