import sys
import csv

import pandas as pd


def main():

    input_file = "./data/wikidata_lang_type_counts_coreutils.noawk"

    def rows():
        with open(input_file, encoding="utf8") as f:
            for line in f:
                try:
                    count, entity_type, language = line.strip().split(
                        maxsplit=2
                    )
                    count = int(count)
                except ValueError:
                    continue
                yield {
                    "language": language,
                    "entity_type": entity_type,
                    "count": count,
                }

    data = pd.DataFrame.from_records(
        sorted(rows(), key=lambda d: f"{d['language']}-{d['entity_type']}")
    )

    data = (
        data.set_index(["language", "entity_type"])
        .unstack()
        .fillna(0)
        .astype(int)
        .applymap(lambda x: f"{x:,}")
    )

    data.to_csv(sys.stdout, sep="\t")


if __name__ == "__main__":
    main()
