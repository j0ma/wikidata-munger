from typing import Dict, Tuple, Type, List

import script_analysis as sa
import pandas as pd
import numpy as np
import click
import orjson

default_human_readable_langs_path = (
    "/home/jonne/wikidata-munger/data/human_readable_lang_names.json"
)

permuter_types = ["comma", "edit_distance"]


def compute_crossing_alignments(
    corpus_df: pd.DataFrame,
    permuter_cls: Type[sa.Permuter],
    language_column: str,
    human_readable_lang_names: Dict[str, str],
    pool_languages: bool = False,
    find_best_token_permutation: bool = False,
):

    corpora = {}
    unique_languages = corpus_df.language.unique()

    if pool_languages:
        print("Creating pooled corpus...")
        big_corpus = sa.Corpus(
            corpus_df=corpus_df,
            language="all",
            normalize_histogram=True,
            ignore_punctuation=True,
            ignore_numbers=False,
            align_with_english=True,
            fastalign_verbose=True,
            permuter_class=permuter_cls,
            find_best_token_permutation=find_best_token_permutation,
        )

        words_per_language: Dict[str, List[sa.TransliteratedName]] = {}

        print("Separating words by language...")

        for w, l, a in zip(
            big_corpus.words, corpus_df[language_column], big_corpus.alignments
        ):
            w.add_alignment(a)

            if l not in words_per_language:
                words_per_language[l] = []

            words_per_language[l].append(w)

        print("Avg. number of crossing alignments per language:")

        for lang, words in words_per_language.items():
            stats = sa.CorpusStatistics(names=words)
            lang_long = human_readable_lang_names.get(lang)
            avg_alignments = stats.mean_cross_alignments
            print(f"{lang_long}\t{avg_alignments}")

    else:
        for language in unique_languages:
            subset = corpus_df[corpus_df[language_column] == language]

            print(f"[{language}] Creating corpus...")
            corpus = sa.Corpus(
                corpus_df=subset,
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

        print("Avg. number of crossing alignments per language:")
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
    "--permute-tokens",
    is_flag=True,
    help="Permute tokens to find the best ordering ",
)
def main(
    input_file,
    language_column,
    random_seed,
    permuter_type,
    human_readable_langs_path,
    pool_languages,
    debug_mode,
    permute_tokens,
):

    # set seed
    np.random.seed(random_seed)

    # read in corpus and subset
    corpus_df = pd.read_csv(
        input_file,
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
        corpus_df = corpus_df.head()

    with open(human_readable_langs_path, encoding="utf8") as f:
        human_readable_lang_names = orjson.loads(f.read())

    permuter_class = {
        "comma": sa.FirstCommaBasedPermuter,
        "edit_distance": sa.LowestDistancePermuter,
    }[permuter_type]

    compute_crossing_alignments(
        corpus_df,
        permuter_class,
        language_column,
        human_readable_lang_names=human_readable_lang_names,
        pool_languages=pool_languages,
        find_best_token_permutation=permute_tokens,
    )


if __name__ == "__main__":
    main()
