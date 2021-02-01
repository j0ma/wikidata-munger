# Wikidata gazetteer generation

This repository implements ingestion and querying functionality for Wikidata dumps, stored as MongoDB databases.

## Dumping information
If you already have a MongoDB instance running, you can use `wikidata_dump_transliterations.py` to dump transliterations.

```
% python wikidata_dump_transliterations.py --help

Usage: wikidata_dump_transliterations.py [OPTIONS]

Options:
  --mongodb-uri TEXT              MongoDB URI
  --database-name TEXT            Database name
  --collection-name TEXT          Collection name
  --subclass-coll-name TEXT       Subclass collection name
  -f, --output-format [jsonl|csv]
  -o, --output-file TEXT          Output file. If empty or '-', defaults to
                                  stdout.

  -d, --delimiter [,|	]           Delimiter for CSV output. Can be comma or
                                  tab. Defaults to comma.

  -t, --conll-type [PER|LOC|ORG]
  -l, --languages TEXT            Comma-separated list of languages to include
  -n, --num-docs FLOAT            Number of documents to output
  -s, --strict                    Strict mode: Only output transliterations in
                                  languages specified using the -l flag.

  --help                          Show this message and exit.
```

For instance, to find the transliterations of 5 geographical locations in Esperanto, we would run:

```
% python wikidata_dump_transliterations.py -t LOC -l eo -f csv -n 5 --strict

id,alias,language
Q463,Rodano-Alpoj,eo
Q694,Sud-Holando,eo
Q912,Malio,eo
Q1693,Norda Maro,eo
Q2109,Arikio kaj Parinakotio,eo
```

We can also get output in JSONL format:

```
% python wikidata_dump_transliterations.py -t LOC -l eo -f jsonl -n 5 --strict

{"id":"Q463","alias":"Rodano-Alpoj","language":"eo"}
{"id":"Q694","alias":"Sud-Holando","language":"eo"}
{"id":"Q912","alias":"Malio","language":"eo"}
{"id":"Q1693","alias":"Norda Maro","language":"eo"}
{"id":"Q2109","alias":"Arikio kaj Parinakotio","language":"eo"}
```

Note that the `-n` flag applies to number of _documents_, not the number of transliterations.
As a result, the number of output lines may vary when not using the `--strict` flag.

Finally, we can also use UNIX tools to get the list of languages.
For example, if we have languages specified in a file `languages`:

```
fi
eo
en
de
```

We may filter for those languages simply by using `tr` to turn the file into a comma-separated list:

```
% python wikidata_dump_transliterations.py \
    -l $(tr "\n" "," < languages | sed s/,$//g) \
    -t PER -f csv -o - -n 5 --strict

id,alias,language
Q1936,Djibril Cissé,en
Q1936,Djibril Cissé,de
Q1936,Djibril Cissé,fi
Q4671,Matthew Kneale,de
Q4671,Matthew Kneale,en
Q4671,Matthew Kneale,fi
Q4926,Birger Kildal,en
Q4926,Birger Kildal,de
Q5380,Robert de Boron,en
Q5380,Robert de Boron,de
Q5380,Robert de Boron,eo
Q5380,Robert de Boron,fi
Q5594,Antonello da Messina,en
Q5594,Antonello da Messina,de
Q5594,Antonello da Messina,fi
Q5594,Antonello da Messina,eo
```

## Notes / todo
- [] Figure out a better way to map to CoNLL types
- [] Figure out a list of African languages
