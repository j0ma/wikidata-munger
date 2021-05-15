import click
import pandas as pd


@click.command()
@click.option(
    "--count-table-path",
    help="Path to CSV of counts per language/entity type",
    required=True,
)
@click.option(
    "--entropy-table-path",
    help="Path to CSV of entropy per language",
    required=True,
)
@click.option(
    "--collapse-types",
    is_flag=True,
    help="Collapse entity types and report counts per language instead of language and type",
)
def main(count_table_path, entropy_table_path, collapse_types):

    count_table = pd.read_csv(
        count_table_path, sep="," if count_table_path.endswith("csv") else "\t"
    )

    entropy_table = pd.read_csv(
        entropy_table_path,
        sep="," if entropy_table_path.endswith("csv") else "\t",
    )

    print(count_table)
    print(entropy_table)


if __name__ == "__main__":
    main()
