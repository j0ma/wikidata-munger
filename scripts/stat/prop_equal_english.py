import pandas as pd
import click


@click.command()
@click.option(
    "--input-file",
    "-i",
    type=click.File("r"),
    default="-",
    help="Path to TSV input file (dump)",
)
@click.option(
    "--output-file",
    "-o",
    type=click.File("w"),
    default="-",
)
def main(input_file, output_file):

    dump = pd.read_csv(
        input_file, sep="," if str(input_file).endswith(".csv") else "\t"
    )
    dump["equals_english"] = (dump.eng == dump.alias).astype(int)
    equal_english_table = (
        dump.groupby("language").equals_english.mean().round(2)
    )

    equal_english_table.to_csv(
        output_file, sep="," if str(output_file).endswith(".csv") else "\t"
    )


if __name__ == "__main__":
    main()
