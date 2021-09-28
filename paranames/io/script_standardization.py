#!/usr/bin/env python

from typing import Dict, Tuple, Generator
from functools import partial
from pathlib import Path
import tempfile

import click
import pandas as pd
from tqdm import tqdm
from paranames.util import read, write
import paranames.util.script as s
from p_tqdm import p_map


vote_aggregation_methods = set(["baseline", "all", "any", "majority_vote"])


def tag_and_split_names(
    language_subset,
    aggregation_method: str,
    critical_value: float = 0.1,
    language_column: str = "language",
    alias_column: str = "alias",
    id_column: str = "wikidata_id",
    english_text_column: str = "eng",
    strip: bool = True,
):

    language, subset = language_subset

    unicode_analyzer = s.UnicodeAnalyzer(
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

    print(f"[{language}] Loading names...")
    subset_names = name_loader(subset)

    print(f"[{language}] Finding most common Unicode block")

    most_common_block = (
        subset[alias_column]
        .apply(unicode_analyzer.most_common_unicode_block)
        .value_counts()
        .idxmax()
    )

    # anomalous if most common unicode block is not expected one
    incorrect_block_tagger = s.IncorrectBlockTagger(expected_block=most_common_block)

    # anomalous if given block is missing
    missing_block_tagger = s.MissingBlockTagger(missing_block=most_common_block)

    # anomalous if JSD from language prototype is greater than a critical value
    prototype = "".join(str(s) for s in subset[alias_column])
    distance_based_tagger = s.JSDTagger(
        per_language_distribution=unicode_analyzer.unicode_block_histogram(prototype),
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
    kept_names = subset[~subset["anomalous"]]
    return filtered_names, kept_names


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
    num_workers: int = 32,
    *args,
    **kwargs,
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    _tag_and_split_names = partial(
        tag_and_split_names,
        aggregation_method=aggregation_method,
        critical_value=critical_value,
        language_column=language_column,
        alias_column=alias_column,
        id_column=id_column,
        english_text_column=english_text_column,
        strip=strip,
    )

    output_chunks = []
    filtered_chunks = []
    tag_and_split_names_output = p_map(
        _tag_and_split_names,
        slice_by_column(data, language_column),
        num_cpus=num_workers,
    )

    for filtered_names, kept_names in tag_and_split_names_output:
        filtered_chunks.append(filtered_names)
        output_chunks.append(kept_names)

    output = pd.concat(output_chunks, ignore_index=True)
    filtered = pd.concat(filtered_chunks, ignore_index=True)

    return output, filtered


def validate_name(
    name: str, language: str, allowed_scripts: Dict[str, Dict[str, str]]
) -> bool:
    ua = s.UnicodeAnalyzer()

    if language not in allowed_scripts:
        return True

    return ua.most_common_unicode_block(name) in allowed_scripts[language]


def baseline_script_standardization(
    data, scripts_file, alias_column, language_column, *args, **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    scripts = read(scripts_file, "tsv")
    allowed_scripts_per_lang = {
        lang: set(scr.split(", "))
        for lang, scr in zip(scripts.language_code, scripts.scripts_to_keep)
    }

    valid_rows_mask = pd.Series(
        [
            validate_name(
                name=row[alias_column],
                language=row[language_column],
                allowed_scripts=allowed_scripts_per_lang,
            )
            for ix, row in tqdm(data.iterrows(), total=data.shape[0])
        ],
        index=data.index,
    )

    print("No. of valid rows: {} / {}".format(sum(valid_rows_mask), data.shape[0]))

    valid = data[valid_rows_mask]
    filtered = data[~valid_rows_mask]

    return valid, filtered


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
@click.option(
    "--vote-aggregation-method",
    default="majority_vote",
    type=click.Choice(vote_aggregation_methods),
    help="Aggregation function to use in script standardization. (Default: majority vote)",
)
@click.option("--write-filtered-names", is_flag=True)
@click.option("--filtered-names-output-file", default="")
@click.option("--compute-script-entropy", is_flag=True)
@click.option(
    "--scripts-file",
    "-s",
    default="~/paranames/data/anecdata/script_standardization/lang_codes_with_longname_and_scripts",
)
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
    vote_aggregation_method,
    write_filtered_names,
    filtered_names_output_file,
    compute_script_entropy,
    scripts_file,
):

    # read in data and sort it by language
    data = read(input_file, io_format=io_format)

    # need to sort by language to ensure ordered chunks
    data = data.sort_values(language_column)

    if vote_aggregation_method == "baseline":
        data, filtered = baseline_script_standardization(
            data,
            id_column=id_column,
            type_column=type_column,
            language_column=language_column,
            aggregation_method=vote_aggregation_method,
            num_workers=num_workers,
            chunksize=chunksize,
            scripts_file=scripts_file,
            alias_column=alias_column,
        )
    else:
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

    # write to disk
    write(data, output_file, io_format=io_format)


if __name__ == "__main__":
    main()
