#!/usr/bin/env python



@click.command()
@click.option("--input-file", "-i", required=True)
@click.option("--output-file", "-o", required=True)
@click.option("--io-format", "-f", default="tsv")
@click.option(
    "--scripts-file",
    "-s",
    default="./data/anecdata/script_standardization/lang_codes_with_longname_and_scripts",
)
@click.option("--alias-column", "-a", default="alias")
@click.option("--language-column", "-l", default="language")
def main(
    input_file,
    output_file,
    io_format,
    scripts_file,
    alias_column,
    language_column,
):

    baseline_script_standardization(
        input_file,
        output_file,
        io_format,
        scripts_file,
        alias_column,
        language_column,
    )

if __name__ == "__main__":
    main()
