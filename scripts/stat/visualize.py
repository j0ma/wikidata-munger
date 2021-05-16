from pathlib import Path

import click
import pandas as pd
import matplotlib.pyplot as plt

rotation_angle = 90
width, height = 12, 6


def plot_zipf_distribution(
    df,
    n_langs=50,
    title="Count per language",
    use_log_y=False,
    stacked=False,
    save_path="",
    width=12,
    height=6,
):
    out = df.set_index(
        "language" if not stacked else ["language", "type"]
    ).drop("language_code", 1)

    if stacked:
        out = out.unstack(-1).fillna(0).astype(int)
        out.columns = [
            entity_type for _, entity_type in out.columns.to_flat_index()
        ]
        out["total"] = out.sum(axis=1)
        out.sort_values("total", ascending=False, inplace=True)
        out.drop("total", 1, inplace=True)
        out.head(n_langs).plot(
            kind="bar",
            title=title,
            logy=use_log_y,
            stacked=stacked,
            rot=rotation_angle,
            figsize=(width, height),
            xlabel=""
        )
    else:
        out.sort_values("count", ascending=False)["count"].head(n_langs).plot(
            kind="bar",
            title=title,
            logy=use_log_y,
            stacked=stacked,
            rot=rotation_angle,
            figsize=(width, height),
            xlabel=""
        )

    plt.tight_layout()
    if save_path:
        plt.savefig(save_path)
    else:
        plt.show()

    plt.clf()


def plot_entropy_distribution(
    df,
    n_langs=50,
    title="Script entropy per language",
    use_log_y=False,
    save_path="",
    width=12,
    height=6,
):
    df.sort_values("script_entropy", ascending=False).head(n_langs).set_index(
        "language"
    ).script_entropy.plot(
        kind="bar",
        title=title,
        logy=use_log_y,
        rot=rotation_angle,
        figsize=(width, height),
        xlabel=""
    )

    plt.tight_layout()
    if save_path:
        plt.savefig(save_path)

    else:
        plt.show()

    plt.clf()


@click.command()
@click.option(
    "--counts-table-path",
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
    help="Collapse entity types & report aggregated counts per language",
)
@click.option(
    "--log-scale", is_flag=True, help="Use log scale on the y-axis",
)
@click.option(
    "--n-languages",
    help="Number of languages to plot results for.",
    default=50,
)
@click.option(
    "--output-folder",
    help="Folder to output plots. Will be created if doesn't exist.",
    default="",
)
@click.option("--width", default=12)
@click.option("--height", default=6)
def main(
    counts_table_path,
    entropy_table_path,
    collapse_types,
    log_scale,
    n_languages,
    output_folder,
    width,
    height,
):

    count_table = pd.read_csv(
        counts_table_path,
        sep="," if counts_table_path.endswith("csv") else "\t",
    )

    entropy_table = pd.read_csv(
        entropy_table_path,
        sep="," if entropy_table_path.endswith("csv") else "\t",
    )

    count_table = count_table.merge(
        entropy_table[["language", "language_code"]],
        on="language_code",
        how="left",
    )[["language", "language_code", "type", "count"]]

    if output_folder:

        output_folder = Path(output_folder)
        if not output_folder.exists():
            output_folder.mkdir()

        zipf_output_file = output_folder / "zipf_count_distribution.png"
        entropy_output_file = output_folder / "entropy_distribution.png"

    else:
        zipf_output_file = ""
        entropy_output_file = ""

    if collapse_types:
        count_table = (
            count_table.groupby("language_code")["count"].sum().reset_index()
        )
        plot_zipf_distribution(
            count_table,
            n_langs=n_languages,
            output_folder=output_folder,
            save_path=zipf_output_file,
            use_log_y=log_scale,
            width=width,
            height=height,
        )

    else:
        plot_zipf_distribution(
            count_table,
            stacked=True,
            n_langs=n_languages,
            save_path=zipf_output_file,
            use_log_y=log_scale,
            width=width,
            height=height,
        )

    plot_entropy_distribution(
        entropy_table,
        n_langs=n_languages,
        save_path=entropy_output_file,
        use_log_y=log_scale,
        width=width,
        height=height,
    )


if __name__ == "__main__":
    main()
