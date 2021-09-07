#!/usr/bin/env python

from typing import Dict, Tuple, Generator
import itertools as it
from pathlib import Path
import tempfile

import click
import numpy as np
import pandas as pd
from tqdm import tqdm
from paranames.util import read, write
import paranames.util.script as s
from p_tqdm import p_map
import orjson

vote_aggregation_methods = set(["all", "any", "majority_vote", "none"])


def slice_by_column(
    data: pd.DataFrame, column: str
) -> Generator[Tuple[str, pd.DataFrame], None, None]:
    """Yields slices of data frame based on values of column.

    Note: assumes values of column are in sorted order
    """
    unique_values = data[column].unique()

    for val in unique_values:
        yield val, data[data[column] == val]


def standardize_script(
    data: pd.DataFrame,
    aggregation_method: str,
    critical_value: float = 0.1,
    language_column: str = "language",
    alias_column: str = "alias",
    id_column: str = "wikidata_id",
    english_text_column: str = "eng",
    strip: bool = True,
    *args,
    **kwargs,
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    ua = s.UnicodeAnalyzer(
        strip=strip,
        ignore_punctuation=True,
        ignore_numbers=True,
        normalize_histogram=True,
    )

    name_loader = s.TransliteratedNameLoader(
        language_column=language_column,
        debug_mode=False,
        name_column=alias_column,
        wikidata_id_column=id_column,
        english_column=english_text_column,
    )

    clean_names_all: List[str] = []

    output_chunks = []
    filtered_chunks = []

    for language, subset in tqdm(
        slice_by_column(data, language_column),
    ):

        print(f"[{language}] Loading names...")

        subset_names = name_loader(subset)

        print(f"[{language}] Finding most common Unicode block")

        most_common_block = (
            subset[alias_column]
            .apply(ua.most_common_unicode_block)
            .value_counts()
            .idxmax()
        )

        # anomalous if most common unicode block is not expected one
        incorrect_block_tagger = s.IncorrectBlockTagger(
            expected_block=most_common_block
        )

        # anomalous if given block is missing
        missing_block_tagger = s.MissingBlockTagger(
            missing_block=most_common_block
        )

        # anomalous if JSD from language prototype is greater than a critical value
        prototype = "".join(str(s) for s in subset[alias_column])
        distance_based_tagger = s.JSDTagger(
            per_language_distribution=ua.unicode_block_histogram(prototype),
            critical_value=critical_value,
            distance_measure="jensen_shannon",
        )

        hiragana_katakana_tagger = s.HiraganaKatakanaTagger()
        cjk_tagger = s.CJKTagger()

        aggregated_tagger = s.AggregatedTagger(
            taggers=[
                incorrect_block_tagger,
                missing_block_tagger,
                distance_based_tagger,
                hiragana_katakana_tagger,
                cjk_tagger,
            ],
            aggregation_method=aggregation_method,
        )

        print(f"[{language}] Tagging names...")
        tagged_names = aggregated_tagger(subset_names)
        subset[alias_column] = [n.text for n in tagged_names]
        subset.loc[:, "anomalous"] = [n.anomalous for n in tagged_names]

        filtered_names = subset[subset["anomalous"]]
        filtered_chunks.append(filtered_names)

        kept_names = subset[~subset["anomalous"]]
        output_chunks.append(kept_names)

    output = pd.concat(output_chunks, ignore_index=True)
    filtered = pd.concat(filtered_chunks, ignore_index=True)

    return output, filtered


def standardize_names(
    data: pd.DataFrame,
    language_column: str,
    alias_column: str,
    id_column: str,
    num_workers: int,
    chunksize: int,
    human_readable_lang_names: Dict[str, str],
    permuter_type: str,
    debug_mode: bool = False,
    chunk_rows: bool = False,
    corpus_require_english: bool = False,
    corpus_filter_blank: bool = False,
    *args,
    **kwargs,
) -> pd.DataFrame:

    permuter_class = {
        "comma": s.PermuteFirstComma,
        "edit_distance": s.PermuteLowestDistance,
        "remove_parenthesis_permute_comma": s.RemoveParenthesisPermuteComma,
        "remove_parenthesis_edit_distance": s.RemoveParenthesisPermuteLowestDistance,
        "remove_parenthesis": s.ParenthesisRemover,
    }[permuter_type]

    num_rows, num_columns = data.shape

    if chunk_rows:
        corpus_chunks = (chunk for chunk in np.array_split(data, chunksize))
    else:
        corpus_chunks = (chunk for chunk in (data,))

    name_loader = s.TransliteratedNameLoader(
        language_column=language_column,
        wikidata_id_column=id_column,
        debug_mode=False,
    )
    names = list(
        it.chain.from_iterable(
            p_map(name_loader, corpus_chunks, num_cpus=num_workers)
        )
    )

    print("[standardize_names] Creating pooled corpus...")
    pooled_corpus = s.Corpus(
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
        require_english=corpus_require_english,
        filter_out_blank=corpus_filter_blank,
    )

    print(f"[standardize_names] Replacing old names...")
    data[alias_column] = [n.text for n in pooled_corpus.names]

    # finally mask out rows with now empty labels
    labels_nonempty = data[alias_column].apply(lambda s: bool(s))
    data = data[labels_nonempty]

    return data


def apply_entity_disambiguation_rules(
    data: pd.DataFrame,
    id_column: str = "wikidata_id",
    type_column: str = "type",
) -> pd.DataFrame:

    # count how many types for each id
    id_to_ntypes_df = (
        data[[id_column, type_column]]
        .drop_duplicates()
        .groupby(id_column)
        .type.size()
        .reset_index()
        .rename(columns={type_column: "n_types"})
    )

    # join this to the original data frame
    old_nrows = data.shape[0]
    data = data.merge(id_to_ntypes_df, on=id_column)

    # if id is in this dict, it will have several types
    id_to_types = (
        data[data.n_types > 1][[id_column, type_column]]
        .drop_duplicates()
        .groupby(id_column)
        .apply(lambda df: "-".join(sorted(df.type.unique())))
    )

    # if there are no duplicates, get rid of n_types column and return

    if id_to_types.empty:
        data = data.drop(columns="n_types")

        return data
    else:
        id_to_types = id_to_types.to_dict()

    # encode actual disambiguation rules
    entity_disambiguation_rules = {
        "LOC-ORG": "LOC",
        "LOC-ORG-PER": "ORG",
        "ORG-PER": "ORG",
        "LOC-PER": "PER",
    }

    # compose the above two relations
    id_to_canonical_type = {
        _id: entity_disambiguation_rules.get(type_str)

        for _id, type_str in id_to_types.items()
    }

    # replace with canonical types, non-ambiguous ones get None
    canonical_types = data[id_column].apply(
        lambda _id: id_to_canonical_type.get(_id, None)
    )

    # put the old non-ambiguous types back in
    new_types = [
        old_type if new_type is None else new_type

        for old_type, new_type in zip(data.type, canonical_types)
    ]

    data[type_column] = new_types

    # finally drop the extra column we created
    data = data.drop(columns="n_types")

    # also drop duplicate rows
    data = data.drop_duplicates()

    # final check to make sure no id has more than 1 type
    assert all(
        data[[id_column, type_column]]
        .drop_duplicates()
        .groupby(id_column)
        .type.size()
        == 1
    )

    # print out some information to the user
    print("Deduplication complete")
    print(f"No. of rows, original: {old_nrows}")
    print(f"No. of rows, filtered: {data.shape[0]}")
    print(f"Rows removed = {old_nrows - data.shape[0]}")

    return data


def filter_am_ti(
    data: pd.DataFrame,
    id_column: str = "wikidata_id",
    type_column: str = "type",
) -> pd.DataFrame:
    with open("./data/am_ti_kept_ids.txt", encoding="utf8") as f:
        am_ti_kept_ids = set([l.strip() for l in f])

        print(
            f"Loaded {len(am_ti_kept_ids)} IDs to keep for Amharic/Tigrinya..."
        )

    # construct a mask for items that are to be excluded

    def should_keep(row):
        if row.language not in ["am", "ti"]:
            return True
        else:
            id_is_suitable = row[id_column] in am_ti_kept_ids
            try:
                alias_not_latin = bool(not row.is_latin)
            except AttributeError:
                alias_not_latin = True  # if is_latin not found

            return id_is_suitable and alias_not_latin

    keep_these = data.apply(should_keep, axis=1)

    filtered = data[keep_these]

    print("Amharic/Tigrinya filtering complete")
    print(f"No. of rows, original: {data.shape[0]}")
    print(f"No. of rows, filtered: {filtered.shape[0]}")
    print(
        f"No of. am/ti rows in filtered: {filtered[filtered.language.isin(['am', 'ti'])].shape[0]}"
    )
    print(f"Rows removed = {data.shape[0] - filtered.shape[0]}")

    return filtered


@click.command()
@click.option("--input-file", "-i")
@click.option("--output-file", "-o")
@click.option("--io-format", "-f", default="tsv")
@click.option("--id-column", "-id", default="wikidata_id")
@click.option("--type-column", "-t", default="type")
@click.option("--alias-column", "-a", default="alias")
@click.option("--english-column", "-e", default="name")
@click.option("--language-column", "-l", default="language")
@click.option("--num-workers", type=int, default=2)
@click.option("--chunksize", type=int, default=15000)
@click.option("--human-readable-langs-path", required=True)
@click.option(
    "--permuter-type", required=True, type=click.Choice(s.permuter_types)
)
@click.option("--corpus-require-english", is_flag=True)
@click.option("--corpus-filter-blank", is_flag=True)
@click.option(
    "--vote-aggregation-method",
    default="majority_vote",
    type=click.Choice(vote_aggregation_methods),
    help="Aggregation function to use in script standardization. (Default: majority vote)",
)
@click.option("--write-filtered-names", is_flag=True)
@click.option("--filtered-names-output-file", default="")
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
    vote_aggregation_method,
    write_filtered_names,
    filtered_names_output_file,
):

    # read in human readable language names
    with open(human_readable_langs_path, encoding="utf8") as f:
        human_readable_lang_names = orjson.loads(f.read())

    # read in data and sort it by language
    data = read(input_file, io_format=io_format)

    # drop rows that are not entities (e.g. P-ids)
    data = data[data[id_column].str.startswith("Q")]

    # change <english_column> to "eng"
    data = data.rename(columns={english_column: "eng"})

    # filter rows using entity disambiguation rules
    data = apply_entity_disambiguation_rules(
        data, id_column=id_column, type_column=type_column
    )

    # need to sort by language to ensure ordered chunks
    data = data.sort_values(language_column)

    # remove wrong script / anomalous names

    if vote_aggregation_method != "none":
        data, filtered = standardize_script(
            data,
            id_column=id_column,
            type_column=type_column,
            language_column=language_column,
            aggregation_method=vote_aggregation_method,
            num_workers=num_workers,
            chunksize=chunksize,
        )

        if write_filtered_names:

            if not filtered_names_output_file:
                _, filtered_names_output_file = tempfile.mkstemp()
            filtered_names_path = Path(filtered_names_output_file)
            containing_folder = filtered_names_path.parents[0]

            if not containing_folder.exists():
                filtered_names_path.mkdir(parents=True)

            write(
                filtered,
                filtered_names_path,
                io_format="tsv",
            )

            print(f"Filtered names written to {filtered_names_output_file}")

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
    )

    # write to disk
    write(data, output_file, io_format=io_format)


if __name__ == "__main__":
    main()
