#!/usr/bin/env python

from typing import Dict
import itertools as it
from p_tqdm import p_map
import orjson

import click
import numpy as np
import pandas as pd
from tqdm import tqdm
from sklearn.metrics import classification_report
from flyingsquid.label_model import LabelModel
import paranames.io.wikidata_helpers as wh
import paranames.analysis.script_analysis as sa


def standardize_script(
    data: pd.DataFrame,
    language_column: str,
    critical_value: float = 0.1,
    *args,
    **kwargs,
) -> pd.DataFrame:

    return data

    unique_languages = set(data[language_column].unique())

    anomalous = {}
    corpora = {}

    # TODO: implement this
    # print("Reading in hand-labeled anomalous data, if any")

    # for language in unique_languages:
    # try:
    # anomalous[language] = {
    # line.split("\t")[0].strip()

    # for line in open(
    # Path(anomalous_data_folder) / f"{language}_anomalous.txt"
    # )
    # }
    # except:
    # continue

    for language in unique_languages:
        print(f"[{language}] Creating corpus...")
        subset = data[data.language == language]
        corpora[language] = sa.Corpus(
            out_folder=(
                Path(output_folder)
                or Path(f"/tmp/flyingsquid-test/{language}")
            ),
            names=[
                sa.TransliteratedName(
                    text=row[alias_column],
                    language=language,
                    english_text=row[english_text_column],
                    wikidata_id=row[id_column],
                    unicode_analyzer=ua,
                    is_unchanged=True,
                )
                for _, row in subset.iterrows()
            ],
            language=language,
            normalize_histogram=True,
            ignore_punctuation=True,
            ignore_numbers=False,
        )

    for language, corpus in corpora.items():
        anomalous_words = anomalous.get(language, {})

        if not anomalous_words:
            names_in_this_lang = set(
                data[data.language == language][alias_column].unique()
            )
            names_in_other_langs = set(
                data[data.language != language][alias_column].unique()
            )

            non_overlapping_names = names_in_other_langs - names_in_this_lang

            anomalous_words = np.random.choice(
                np.array(list(non_overlapping_names)),
                size=n_noise_words,
                replace=False,
            )

        corpus.add_words(
            [
                sa.TransliteratedName(
                    text=w,
                    language=language,
                    noise_sample=True,
                    unicode_analyzer=ua,
                    anomalous=True,
                    is_unchanged=True,
                )
                for w in anomalous_words
            ]
        )

        anomalous[language] = set(anomalous_words)

    for language, corpus in corpora.items():

        print(f"[{language}] Done. Predicting...")

        # anomalous if most common unicode block is not expected one
        incorrect_block_tagger = sa.IncorrectBlockTagger(
            expected_block=corpus.most_common_unicode_block
        )

        # anomalous if given block is missing
        missing_block_tagger = sa.MissingBlockTagger(
            missing_block=corpus.most_common_unicode_block
        )

        # anomalous if JSD from language prototype is greater than a critical value
        distance_based_tagger = sa.JSDTagger(
            per_language_distribution=corpus.prototype,
            critical_value=critical_value,
            distance_measure="jensen_shannon",
        )

        hiragana_katakana_tagger = sa.HiraganaKatakanaTagger()
        cjk_tagger = sa.CJKTagger()

        true_labels = []

        for name in corpus.names:
            if name.anomalous or name.text in anomalous.get(language, {}):
                true_labels.append(1)
            elif name.anomalous == None:
                true_labels.append(0)
            else:
                true_labels.append(-1)

        label_priors = pd.Series(true_labels).value_counts(normalize=True)
        print(f"Label priors:\n{label_priors}")

        cr = lambda pred: classification_report(
            y_true=true_labels, y_pred=pred
        )

        def get_preds(tagger):
            return list(yield_preds(tagger))

        def yield_preds(tagger):
            for w in tagger(corpus.names):
                if w.anomalous is None:
                    yield 0
                elif w.anomalous:
                    yield 1
                else:
                    yield -1

        ibt_preds = get_preds(incorrect_block_tagger)
        mbt_preds = get_preds(missing_block_tagger)
        dbt_preds = get_preds(distance_based_tagger)
        hk_preds = get_preds(hiragana_katakana_tagger)
        cjk_preds = get_preds(hiragana_katakana_tagger)

        noisy_votes = np.vstack(
            [ibt_preds, mbt_preds, dbt_preds, hk_preds, cjk_preds]
        ).T
        num_labeling_functions = noisy_votes.shape[1]

        label_model = LabelModel(num_labeling_functions)
        label_model.fit(noisy_votes)

        preds = label_model.predict(noisy_votes).reshape(
            np.array(true_labels).shape
        )

        tagged_names = [
            sa.TransliteratedName(
                text=w.text,
                unicode_analyzer=w.unicode_analyzer,
                anomalous=bool(pred > 0),
                language=w.language,
                noise_sample=w.noise_sample,
                is_unchanged=w.is_unchanged,
            )
            for w, pred in zip(corpus.names, preds)
        ]

        print("Here is what FlyingSquid missed:")

        for (name, pred) in zip(corpus.names, preds):
            gold = name.anomalous
            neg = name.noise_sample
            pred = bool(pred > 0)

            if gold and not pred:
                print(f"[{language}] Anomalous but not tagged: {name.text}")
            elif pred and gold == False and not neg:
                print(f"[{language}] Non-anomalous but tagged: {name.text}")

        corpus.names = tagged_names

    return data


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
        require_english=corpus_require_english,
        filter_out_blank=corpus_filter_blank,
    )

    print(f"[standardize_names] Replacing old names...")
    print(data.shape, len(names), len(pooled_corpus.names))
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
            # alias_not_eng = row.alias != row.eng
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
    "--permuter-type", required=True, type=click.Choice(sa.permuter_types)
)
@click.option("--corpus-require-english", is_flag=True)
@click.option("--corpus-filter-blank", is_flag=True)
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
):

    # read in human readable language names
    with open(human_readable_langs_path, encoding="utf8") as f:
        human_readable_lang_names = orjson.loads(f.read())

    # read in data
    data = wh.read(input_file, io_format=io_format)

    # drop rows that are not entities (e.g. P-ids)
    data = data[data[id_column].str.startswith("Q")]

    # change <english_column> to "eng"
    data = data.rename(columns={english_column: "eng"})

    # filter rows using entity disambiguation rules
    data = apply_entity_disambiguation_rules(
        data, id_column=id_column, type_column=type_column
    )

    # filter amharic & tigrinya
    data = filter_am_ti(data, id_column=id_column, type_column=type_column)

    # remove wrong script / anomalous names
    data = standardize_script(
        data,
        id_column=id_column,
        type_column=type_column,
        language_column=language_column,
    )

    # standardize names (permute tokens in PER)
    # import ipdb

    # ipdb.set_trace()
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
    wh.write(data, output_file, io_format=io_format)


if __name__ == "__main__":
    main()
