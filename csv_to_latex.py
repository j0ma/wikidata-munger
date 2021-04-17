import sys

import pandas as pd


def main():

    try:
        mode = sys.argv[1]
    except IndexError:
        mode = "csv"

    with sys.stdin as fin, sys.stdout as fout:
        fout.write(pd.read_csv(fin, sep="\t" if mode == "tsv" else ",").to_latex(index=False))


if __name__ == "__main__":
    main()
