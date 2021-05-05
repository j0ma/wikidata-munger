#!/usr/bin/env bash

set -euo pipefail

# Dumping everything, 2/5/2021

#CONLL_ENTITY_TYPE="${1}"
FORMAT="${1}"
LANGS="fi"

for CONLL_ENTITY_TYPE in "PER" "LOC" "ORG"
do
    OUTPUT="/home/jonne/datasets/wikidata/output-format-test/${CONLL_ENTITY_TYPE}.csv" 

    mkdir -p $(dirname ${OUTPUT})

    # dump everything into one file
    python wikidata_dump_transliterations.py \
        --strict \
        -t "${CONLL_ENTITY_TYPE}" \
        -l "${LANGS}" \
        -f "${FORMAT}" \
        -d "tab" \
        -o - | tee "${OUTPUT}"

    # separate by language
    python separate_by_language.py \
        --lang-column "language" \
        --input-file "${OUTPUT}" \
        --use-subfolders \
        --io-format "tsv" \
        --verbose
done
