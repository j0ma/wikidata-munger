import pandas as pd
import click


@click.command()
@click.option(
    "--input-file",
    "-i",
    help="Path to TSV input file (dump)",
)
@click.option(
    "--output-file",
    "-o",
)
def main(input_file, output_file):

    dump = pd.read_csv(
        input_file, sep="," if input_file.endswith(".csv") else "\t"
    )
    dump["equals_english"] = (dump.eng == dump.alias).astype(int)
    equal_english_table = (
        (dump.groupby("language").equals_english.mean().round(2))
        .reset_index()
        .rename(
            columns={
                "language": "language_code",
                "equals_english": "english_match",
            }
        )
        .to_csv(
            output_file,
            sep="," if output_file.endswith(".csv") else "\t",
            index=False,
        )
    )


if __name__ == "__main__":
    main()
