import sys
import csv

import pandas as pd
import scipy.stats as sps


def main():

    input_file = "./data/wikidata_lang_script_counts_coreutils.noawk"

    def rows():
        with open(input_file, encoding="utf8") as f:
            for line in f:
                try:
                    line = line.strip()
                    header, script = line.split('\t')
                    count, language = header.split(maxsplit=1)
                    count = int(count)
                except ValueError:
                    continue
                yield {
                    "language": language,
                    "script": script,
                    "count": count,
                }

    data = pd.DataFrame.from_records(
        sorted(rows(), key=lambda d: f"{d['language']}-{d['script']}")
    )

    #data = data.set_index(["language", "script"]).unstack().fillna(0).astype(int).applymap(lambda x: f"{x:,}")
    entropy = data.groupby(["language"]).apply(lambda df: sps.entropy(df.script.value_counts(normalize=True)))

    with sys.stdout as f:
        entropy.round(4).reset_index().to_csv(f, sep="\t", index=False)


if __name__ == "__main__":
    main()
