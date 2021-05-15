#!/usr/bin/env bash

set -euo pipefail

# Dumping everything, 2/5/2021

LANGS="am,ti"
FORMAT="${1}"
OUTPUT_FOLDER="/home/jonne/datasets/wikidata/am-ti-per-dump-051521/"
COMBO_OUTPUT="${OUTPUT_FOLDER}/combined.csv"

mkdir -p $OUTPUT_FOLDER

run () {

    CONLL_TYPE=$1
    OUTPUT="${OUTPUT_FOLDER}/${CONLL_TYPE}.csv"

    # dump everything into one file
    python scripts/io/wikidata_dump_transliterations.py \
        --strict \
        -t "${CONLL_TYPE}" \
        -l "${LANGS}" \
        -L 'ti' \
        -f "${FORMAT}" \
        -o - | tee "${OUTPUT}"

    # separate by language
    #python scripts/io/separate_by_language.py \
        #--lang-column "language" \
        #--input-file "${OUTPUT}" 
}

for conll_type in "PER" "LOC" "ORG"
do
    run $conll_type
done

cat $OUTPUT_FOLDER/PER.csv > $COMBO_OUTPUT
tail +2 $OUTPUT_FOLDER/LOC.csv >> $COMBO_OUTPUT
tail +2 $OUTPUT_FOLDER/ORG.csv >> $COMBO_OUTPUT
