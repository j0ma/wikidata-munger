from typing import List

from paranames.io.wikidata_helpers import orjson_dump
import requests
import cssselect
import lxml.html as html
import click
import pandas as pd


@click.command()
@click.option(
    "--url",
    "-u",
    default="https://en.wikipedia.org/wiki/List_of_Wikipedias#List",
    help="URL to scrape",
)
@click.option(
    "--sel",
    "-s",
    default="table.wikitable:nth-child(17)",
    help="CSS Selector for table",
)
@click.option(
    "--columns",
    "-c",
    default="name,language,script,wp_code,active_users,logo",
    help="Comma-separated list of column names",
)
@click.option("--lang-col", default="language")
@click.option("--value-col", default="wp_code")
@click.option("--african-only", is_flag=True)
@click.option("--abbrev-only", is_flag=True)
def main(url, sel, columns, lang_col, value_col, african_only, abbrev_only):
    column_names = columns.split(",")
    tree = html.fromstring(requests.get(url).content)
    table = tree.cssselect(sel)[0]
    rows = table.cssselect("tr")
    cells = [[td.text_content() for td in tr.cssselect("td")] for tr in rows]
    table_rows = cells[1:]
    df = pd.DataFrame.from_records(table_rows, columns=column_names)

    if african_only:

        with open("data/african-languages.txt", encoding="utf-8") as f:
            african_langs = set([line.strip() for line in f.readlines()])

        df = df[df.language.isin(african_langs)].reset_index(drop=True)

    lang_to_wikipedia_code = {
        str(d[lang_col])
        .strip(): str(d[value_col].replace(" (closed)", ""))
        .strip()
        for d in df[[lang_col, value_col]].to_dict("records")
    }

    if abbrev_only:
        print("\n".join(lang_to_wikipedia_code.values()))
    else:
        print(orjson_dump(lang_to_wikipedia_code))


if __name__ == "__main__":
    main()
