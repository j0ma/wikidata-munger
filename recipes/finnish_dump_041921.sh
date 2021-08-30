#!/usr/bin/env bash

set -euo pipefail

# Dumping Finnish, 4/4/2021

LANGS="fi"
FORMAT="tsv"
OUTPUT_FOLDER="${1:-/home/jonne/datasets/wikidata/finnish-dump-041921/}"
COMBO_OUTPUT="${OUTPUT_FOLDER}/finnish.tsv"
DEDUP_OUTPUT="${OUTPUT_FOLDER}/finnish.dedup.tsv"
MATRIX_OUTPUT="${OUTPUT_FOLDER}/finnish.matrix.tsv"

mkdir -p $OUTPUT_FOLDER

run () {

    CONLL_TYPE=$1
    OUTPUT="${OUTPUT_FOLDER}/${CONLL_TYPE}.tsv"

    # dump everything into one file
    python wikidata_dump_transliterations.py \
        --strict \
        --database-name wikidata_db \
        --collection-name wikidata_simple_preconference \
        -t "${CONLL_TYPE}" \
        -l "${LANGS}" \
        -f "${FORMAT}" \
        -d "tab" \
        -o - | tee "${OUTPUT}"

}

# first extract everything for each type
for conll_type in "PER" "LOC" "ORG"
do
    run $conll_type
done

# combine all the tsvs into one big tsv
cat $OUTPUT_FOLDER/PER.tsv > $COMBO_OUTPUT
tail +2 $OUTPUT_FOLDER/LOC.tsv >> $COMBO_OUTPUT
tail +2 $OUTPUT_FOLDER/ORG.tsv >> $COMBO_OUTPUT

# deduplicate the rows by using "trumping rules" to break ties etc.
python postprocess.py \
    -f $FORMAT \
    -i $COMBO_OUTPUT \
    -o $DEDUP_OUTPUT

# finally create the matrix form
python create_matrix.py \
    -i $DEDUP_OUTPUT \
    -o $MATRIX_OUTPUT
