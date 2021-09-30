#!/usr/bin/env python

# Computes basic statistics based on filtered names and

from pathlib import Path

from sklearn.metrics import confusion_matrix
from paranames.util import read, orjson_dump
import pandas as pd
import numpy as np
import click

SCRIPT_STANDARDIZATION_CHUNKSIZE = 10
NAME_STANDARDIZATION_CHUNKSIZE = 30

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)


def script_standardization_stats(
    filtered_names_tsv: pd.DataFrame,
    script_anecdata_folder: str,
    id_column: str,
    language_column: str,
) -> None:
    confusion_matrices = []
    unique_languages = set()
    script_anecdata_path = Path(script_anecdata_folder)
    script_anecdata_files = script_anecdata_path.glob("anecdata_*.tsv")
    for f in script_anecdata_files:
        unique_languages.add(str(f.name).replace("anecdata_", "").replace(".tsv", ""))
    removed_ids_per_lang = {
        language: set(
            filtered_names_tsv[filtered_names_tsv[language_column] == language][
                id_column
            ]
        )
        for language in unique_languages
    }

    stats = {
        "global": {
            "n_correctly_removed": 0,
            "n_incorrectly_removed": 0,
            "n_unknown": 0,
            "n_should_remove": 0,
            "n_should_not_remove": 0,
        }
    }
    for lang in sorted(unique_languages):
        anecdata_tsv_path = script_anecdata_path / f"anecdata_{lang}.tsv"
        tsv = read(anecdata_tsv_path, "tsv", chunksize=SCRIPT_STANDARDIZATION_CHUNKSIZE)
        dont_remove_ids, should_remove_ids = set(next(tsv)[id_column]), set(
            next(tsv)[id_column]
        )
        removed_ids = removed_ids_per_lang[lang]
        correctly_removed = should_remove_ids & removed_ids
        incorrectly_removed = dont_remove_ids & removed_ids
        n_correctly_removed = len(correctly_removed)
        n_incorrectly_removed = len(incorrectly_removed)
        n_unknown = len(removed_ids - correctly_removed - incorrectly_removed)

        stats[lang] = {
            "n_correctly_removed": n_correctly_removed,
            "n_incorrectly_removed": n_incorrectly_removed,
            "n_unknown": n_unknown,
            "n_should_remove": len(should_remove_ids),
            "n_should_not_remove": len(dont_remove_ids),
        }
        stats["global"]["n_correctly_removed"] += n_correctly_removed
        stats["global"]["n_incorrectly_removed"] += n_incorrectly_removed
        stats["global"]["n_unknown"] += n_unknown
        stats["global"]["n_should_remove"] += len(should_remove_ids)
        stats["global"]["n_should_not_remove"] += len(dont_remove_ids)

        # scikit-learn confusion matrix
        y_true = [False for _id in dont_remove_ids] + [
            True for _id in should_remove_ids
        ]
        y_pred = [_id in removed_ids for _id in dont_remove_ids] + [
            _id in removed_ids for _id in should_remove_ids
        ]

        C = confusion_matrix(y_true=y_true, y_pred=y_pred)
        confusion_matrices.append(C)
        print(f"[{lang}] Confusion matrix:")
        print(C)
        print()

    # print(orjson_dump(stats))
    confusion_matrices = np.array(confusion_matrices)
    print("Average confusion matrix:")
    print(confusion_matrices.mean(axis=0))


@click.command()
@click.option("--script-standardization", is_flag=True)
@click.option("--name-standardization", is_flag=True)
@click.option("--filtered-names-tsv", type=click.Path())
@click.option("--script-anecdata-folder", type=click.Path())
@click.option("--name-anecdata-folder", type=click.Path())
@click.option("--id-column", type=str, default="wikidata_id")
@click.option("--language-column", type=str, default="language")
def main(
    script_standardization,
    name_standardization,
    filtered_names_tsv,
    script_anecdata_folder,
    name_anecdata_folder,
    id_column,
    language_column,
):

    filtered_names_tsv_file = read(filtered_names_tsv, "tsv")

    if script_standardization:
        script_standardization_stats(
            filtered_names_tsv_file,
            script_anecdata_folder,
            id_column=id_column,
            language_column=language_column,
        )


if __name__ == "__main__":
    main()
