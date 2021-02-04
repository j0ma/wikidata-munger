#!/usr/bin/env bash

set -euo pipefail

# Dumping everything, 2/2/2021

CONLL_TYPE="${1}"
LANGS="af,xh,zu,nso,tn,ts,st,ve,ss,nd"
FORMAT="${2}"
OUTPUT="/home/jonne/datasets/wikidata/african-language-dump-020221/${CONLL_TYPE}.csv" 

mkdir -p $(dirname ${OUTPUT})

# dump everything into one file
python wikidata_dump_transliterations.py \
    --strict \
    -t "${CONLL_TYPE}" \
    -l "${LANGS}" \
    -f "${FORMAT}" \
    -o - | tee "${OUTPUT}"

# separate by language
python separate_by_language.py \
    --lang-column "language"
    --input-file "${OUTPUT}" \ 
