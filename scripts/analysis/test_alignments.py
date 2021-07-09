from typing import Dict, Tuple, Type, List, Iterable
from collections import defaultdict

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
    "remove_parenthesis_permute_comma",
    "remove_parenthesis",
]


def compute_crossing_alignments(
    transliterated_names: Dict[str, List[sa.TransliteratedName]],
    permuter_cls: Type[sa.NameProcessor],
    language_column: str,
    human_readable_lang_names: Dict[str, str],
    pool_languages: bool = False,
    find_best_token_permutation: bool = False,
):

    unique_languages = set(lang for lang in transliterated_names)
    names = list(
        it.chain.from_iterable(
            name_chunk for lang, name_chunk in transliterated_names.items()
        )
    )

    if pool_languages:
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
            find_best_token_permutation=find_best_token_permutation,
            analyze_unicode=False,
        )

        for n, a in zip(big_corpus.names, big_corpus.alignments):
            n.add_alignment(a)

        print(
            "[compute_crossing_alignments] Avg. number of crossing alignments per language:"
        )

        for lang, names in transliterated_names.items():
            stats = sa.CorpusStatistics(names=names)
            lang_long = human_readable_lang_names.get(lang)
            avg_alignments = stats.mean_cross_alignments
            print(f"{lang_long}\t{avg_alignments}")

    else:
        corpora = {}

        for language, names_subset in transliterated_names.items():
            # names_subset = [n for n in names if n.language == language]

            print(f"[{language}] Creating corpus...")
            corpus = sa.Corpus(
                names=names_subset,
                language=language,
                normalize_histogram=True,
                ignore_punctuation=True,
                ignore_numbers=False,
                align_with_english=True,
                permuter_class=permuter_cls,
                find_best_token_permutation=find_best_token_permutation,
            )
            corpora[language] = corpus

        mean_cross_alignments = {
            language: corpora[language].mean_cross_alignments

            for language in unique_languages
        }

        print(
            "[compute_crossing_alignments] Avg. number of crossing alignments per language:"
        )
        avg_cross_alignments_per_language = pd.Series(mean_cross_alignments)

        for (
            lang,
            avg_alignments,
        ) in avg_cross_alignments_per_language.sort_values(
            ascending=False
        ).items():
            lang_long = human_readable_lang_names.get(lang)
            print(f"{lang_long}\t{avg_alignments}")


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
        n_debug = 5
        corpus_chunks = [
            chunk for chunk, _ in zip(corpus_chunks, range(n_debug))
        ]

    name_loader = sa.TransliteratedNameLoader(
        language_column=language_column, debug_mode=False  # debug_mode,
    )

    print(f"Name Loader: {name_loader}")

    print(f"Loading names using p_map and {num_workers} workers...")
    nested_names = p_map(
        name_loader, list(corpus_chunks), num_cpus=num_workers
    )

    transliterated_names = defaultdict(list)

    for name_chunk in nested_names:
        for name in name_chunk:
            transliterated_names[name.language].append(name)

    compute_crossing_alignments(
        transliterated_names,
        permuter_class,
        language_column,
        human_readable_lang_names=human_readable_lang_names,
        pool_languages=pool_languages,
        find_best_token_permutation=permute_tokens,
    )


if __name__ == "__main__":
    main()