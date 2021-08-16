from typing import Dict, Tuple, Type, List, Iterable
from collections import defaultdict, Counter

import script_analysis as sa
import pandas as pd
import numpy as np
import click
import orjson

from p_tqdm import p_map
import itertools as it

default_human_readable_langs_path = (
    "/home/jonne/wikidata-munger/data/human_readable_lang_names.json"
)

permuter_types = [
    "comma",
    "edit_distance",
    "remove_parenthesis",
    "remove_parenthesis_edit_distance",
    "remove_parenthesis_permute_comma",
]


def compute_crossing_alignments_pooled(
    names: List[sa.TransliteratedName],
    permuter_cls: Type[sa.NameProcessor],
    language_column: str,
    human_readable_lang_names: Dict[str, str],
    pool_languages: bool = False,
    find_best_token_permutation: bool = False,
    preserve_fastalign_output: bool = False,
    debug_mode: bool = False,
    write_permuted_names: bool = True,
    names_output_folder: str = "/tmp/test_alignments_names",
):

    print("[compute_crossing_alignments] Creating pooled corpus...")
    big_corpus = sa.Corpus(
        names=names,
        language="all",
        normalize_histogram=True,
        ignore_punctuation=True,
        ignore_numbers=False,
        align_with_english=True,
        fastalign_verbose=True,
        permuter_class=permuter_cls,
        permuter_inplace=True,
        find_best_token_permutation=find_best_token_permutation,
        analyze_unicode=False,
        preserve_fastalign_output=preserve_fastalign_output,
        debug_mode=debug_mode,
        out_folder=names_output_folder,
    )

    if write_permuted_names:
        print(
            f"[compute_crossing_alignments] Writing names out to {big_corpus.out_folder}:"
        )
        big_corpus.write_permutations()

    print(
        "[compute_crossing_alignments] Avg. number of crossing alignments per language:"
    )

    for lang, stats_per_lang in big_corpus.stats.items():
        lang_long = human_readable_lang_names.get(lang, lang)
        avg_alignments = stats_per_lang.mean_cross_alignments
        print(f"{lang_long}\t{avg_alignments}")

    print(
        "[compute_crossing_alignments] Number of permuted / surviving words per language:"
    )

    for lang, stats_per_lang in big_corpus.stats.items():
        lang_long = human_readable_lang_names.get(lang, lang)
        total_permuted = stats_per_lang.total_permuted
        total_surviving = stats_per_lang.total_surviving
        print(f"{lang_long}\t{total_permuted}\t{total_surviving}")


def compute_crossing_alignments_unpooled(*args, **kwargs):
    raise NotImplementedError("Unpooled mode deprecated!")


@click.command()
@click.option("--input-file", "-i")
@click.option("--language-column", "-lc", default="language")
@click.option("--random-seed", "-s", type=int, default=1917)
@click.option(
    "--human-readable-langs-path", default=default_human_readable_langs_path
)
@click.option(
    "--permuter-type",
    type=click.Choice(permuter_types),
    default="edit_distance",
)
@click.option(
    "--pool-languages",
    is_flag=True,
    help="Pool all languages when aligning so that only one big model is trained",
)
@click.option(
    "--debug-mode", is_flag=True, help="Debug mode: only use 10 rows of data"
)
@click.option(
    "--parallelize", is_flag=True, help="Parallelize using num_workers CPUs"
)
@click.option(
    "--permute-tokens",
    is_flag=True,
    help="Permute tokens to find the best ordering ",
)
@click.option("--num-workers", type=int, default=2)
@click.option("--chunksize", type=int, default=15000)
@click.option(
    "--preserve-fastalign-output",
    is_flag=True,
    help="Do not delete the alignmer output",
)
@click.option("--num-debug-chunks", type=int, default=pow(10, 10))
@click.option(
    "--write-permuted-names",
    is_flag=True,
    help="Write permuted names to output folder specified by the --names-output-folder flag",
)
@click.option("--names-output-folder", default="/tmp/test_alignments_names")
def main(
    input_file,
    language_column,
    random_seed,
    permuter_type,
    human_readable_langs_path,
    pool_languages,
    debug_mode,
    parallelize,
    permute_tokens,
    num_workers,
    chunksize,
    preserve_fastalign_output,
    num_debug_chunks,
    write_permuted_names,
    names_output_folder,
):

    # set seed
    np.random.seed(random_seed)

    # load human readable language information
    with open(human_readable_langs_path, encoding="utf8") as f:
        human_readable_lang_names = orjson.loads(f.read())

    # get the right class for permuting tokens
    permuter_class = {
        "comma": sa.PermuteFirstComma,
        "edit_distance": sa.PermuteLowestDistance,
        "remove_parenthesis_permute_comma": sa.RemoveParenthesisPermuteComma,
        "remove_parenthesis_edit_distance": sa.RemoveParenthesisPermuteLowestDistance,
        "remove_parenthesis": sa.ParenthesisRemover,
    }[permuter_type]

    # read in corpus and subset
    corpus_chunks = pd.read_csv(
        input_file,
        chunksize=chunksize,
        encoding="utf-8",
        delimiter="\t",
        na_values=set(
            [
                "",
                "#N/A",
                "#N/A N/A",
                "#NA",
                "-1.#IND",
                "-1.#QNAN",
                "-NaN",
                "1.#IND",
                "1.#QNAN",
                "<NA>",
                "N/A",
                "NA",
                "NULL",
                "NaN",
                "n/a",
                "null",
            ]
        ),
        keep_default_na=False,
    )

    if debug_mode:
        corpus_chunks = [
            chunk for chunk, _ in zip(corpus_chunks, range(num_debug_chunks))
        ]

    name_loader = sa.TransliteratedNameLoader(
        language_column=language_column, debug_mode=False  # debug_mode,
    )

    print(f"Name Loader: {name_loader}")

    print(f"Loading names using p_map and {num_workers} workers...")
    names = list(
        it.chain.from_iterable(
            p_map(name_loader, corpus_chunks, num_cpus=num_workers)
        )
    )

    if pool_languages:
        compute_crossing_alignments_pooled(
            names,
            permuter_class,
            language_column,
            human_readable_lang_names=human_readable_lang_names,
            pool_languages=pool_languages,
            find_best_token_permutation=permute_tokens,
            preserve_fastalign_output=preserve_fastalign_output,
            debug_mode=debug_mode,
            write_permuted_names=write_permuted_names,
            names_output_folder=names_output_folder,
        )
    else:

        raise NotImplementedError("Unpooled mode deprecated!")


if __name__ == "__main__":
    main()