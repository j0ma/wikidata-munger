from typing import Dict
from pathlib import Path

import pandas as pd
import click
import orjson

import script_analysis as sa

from unicodeblock import blocks
import dictances as dt

import flyingsquid as fs

import numpy as np

from sklearn.metrics import recall_score, classification_report
from flyingsquid.label_model import LabelModel

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

distance_measure_choices = [
    "kullback_leibler",
    "jensen_shannon",
    "bhattacharyya",
]


@click.command()
@click.option("--input-file", "-i", required=True)
@click.option("--output-folder", "-o", required=True, default="")
@click.option("--io-format", "-f", default="tsv")
@click.option(
    "--distance-measure",
    "-d",
    default="jensen_shannon",
    type=click.Choice(distance_measure_choices),
)
@click.option(
    "--anomalous-data-folder",
    "-a",
    default="/home/jonne/datasets/wikidata/flyingsquid/",
)
@click.option("--n-noise-words", type=int, default=1000)
@click.option("--critical-value", "-c", default=0.1)
@click.option("--strip", is_flag=True)
@click.option("--no-normalize", is_flag=True)
@click.option("--ignore-punctuation", is_flag=True)
def main(
    input_file: str,
    output_folder: str,
    io_format: str,
    distance_measure: str,
    anomalous_data_folder: str,
    n_noise_words: int,
    critical_value: float,
    strip: bool,
    no_normalize: bool,
    ignore_punctuation: bool,
) -> None:

    # only csv/tsv supported for now
    assert io_format in ["csv", "tsv"]

    # unicode analyzer
    ua = sa.UnicodeAnalyzer(
        strip=strip,
        normalize_histogram=not no_normalize,
        ignore_punctuation=ignore_punctuation,
    )

    print("Reading in corpus")
    data = pd.read_csv(input_file, sep="\t" if io_format == "tsv" else ",")
    unique_languages = data.language.unique()

    anomalous = {}

    print("Reading in hand-labeled anomalous data, if any")

    for language in unique_languages:
        try:
            anomalous[language] = {
                line.split("\t")[0].strip()

                for line in open(
                    Path(anomalous_data_folder) / f"{language}_anomalous.txt"
                )
            }
        except:
            continue

    corpora = {}

    for language in unique_languages:
        print(f"[{language}] Creating corpus...")
        corpora[language] = sa.Corpus(
            out_folder=(
                Path(output_folder)
                or Path(f"/tmp/flyingsquid-test/{language}")
            ),
            corpus_df=data[data.language == language],
            language=language,
            normalize_histogram=True,
            ignore_punctuation=True,
            ignore_numbers=False,
            # align_with_english=True,
        )

    # add in data for negative sampling
    print("Adding in data for negative sampling")

    if n_noise_words > 0:
        for language, corpus in corpora.items():
            anomalous_words = anomalous.get(language, {})

            if not anomalous_words:
                names_in_this_lang = set(
                    data[data.language == language].alias.unique()
                )
                names_in_other_langs = set(
                    data[data.language != language].alias.unique()
                )

                non_overlapping_names = (
                    names_in_other_langs - names_in_this_lang
                )

                anomalous_words = np.random.choice(
                    np.array(list(non_overlapping_names)),
                    size=n_noise_words,
                )

            corpus.add_words(
                [
                    sa.TransliteratedName(
                        text=w,
                        language=language,
                        noise_sample=True,
                        unicode_analyzer=ua,
                        anomalous=True,
                    )

                    for w in anomalous_words
                ]
            )

            anomalous[language] = set(anomalous_words)
    else:
        anomalous[language] = set()

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

        for name in corpus.words:
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
            for w in tagger(corpus.words):
                if w.anomalous is None:
                    yield 0
                elif w.anomalous:
                    yield 1
                else:
                    yield -1

        ibt_preds = get_preds(incorrect_block_tagger)
        # print(f"[{language}] IncorrectBlockTagger: {cr(ibt_preds)}")

        mbt_preds = get_preds(missing_block_tagger)
        # print(f"[{language}] MissingBlockTagger: {cr(mbt_preds)}")

        dbt_preds = get_preds(distance_based_tagger)
        # print(f"[{language}] DistanceBasedTagger: {cr(dbt_preds)}")

        hk_preds = get_preds(hiragana_katakana_tagger)
        # print(f"[{language}] HiraganaKatakanaTagger: {cr(hk_preds)}")

        cjk_preds = get_preds(hiragana_katakana_tagger)
        # print(f"[{language}] CJKTagger: {cr(cjk_preds)}")

        noisy_votes = np.vstack(
            [ibt_preds, mbt_preds, dbt_preds, hk_preds, cjk_preds]
        ).T
        num_labeling_functions = noisy_votes.shape[1]

        label_model = LabelModel(num_labeling_functions)
        label_model.fit(noisy_votes)

        preds = label_model.predict(noisy_votes).reshape(
            np.array(true_labels).shape
        )
        majority_vote_preds = (noisy_votes.mean(axis=1) > 0).astype(int)
        # print(f"[{language}] Majority vote: {cr(majority_vote_preds)}")

        tagged_names = [
            sa.TransliteratedName(
                text=w.text,
                unicode_analyzer=w.unicode_analyzer,
                anomalous=bool(pred > 0),
                language=w.language,
                noise_sample=w.noise_sample,
            )

            for w, pred in zip(corpus.words, majority_vote_preds)
        ]

        # print(f"[{language}] FlyingSquid: {cr(preds)}")

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
        corpus.write_anomaly_info(write_noise_samples=False)


if __name__ == "__main__":
    main()
