#!/usr/bin/env bash

set -euo pipefail

# Dumping german/swedish/yiddish. 040521

LANGS="de,sv,yi"
FORMAT="tsv"
OUTPUT_FOLDER="${1:-/home/jonne/datasets/wikidata/de-sv-yi-dump-040521/}"
COMBO_OUTPUT="${OUTPUT_FOLDER}/de_sv_yi.tsv"
DEDUP_OUTPUT="${OUTPUT_FOLDER}/de_sv_yi.dedup.tsv"
MATRIX_OUTPUT="${OUTPUT_FOLDER}/de_sv_yi.matrix.tsv"

mkdir -p $OUTPUT_FOLDER

run () {

    CONLL_TYPE=$1
    OUTPUT="${OUTPUT_FOLDER}/${CONLL_TYPE}.tsv"

    # dump everything into one file
    python wikidata_dump_transliterations.py \
        --strict \
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
python deduplicate.py \
    -i $COMBO_OUTPUT \
    -o $DEDUP_OUTPUT

# finally create the matrix form
#python create_matrix.py \
    #-i $DEDUP_OUTPUT \
    #-o $MATRIX_OUTPUT

# separate by language
python separate_by_language.py \
    --lang-column "language" \
    --input-file "${DEDUP_OUTPUT}"  \
    --io-format "tsv"
