#!/usr/bin/env bash

set -euo pipefail

# Dumping everything, 2/5/2021

LANGS="aa,af,ak,am,ny,ha,ig,rw,rn,kg,ln,lg,mg,nso,om,sn,so,sw,ti,ts,tn,ve,wo,xh,yo,zu,kea,ada,fon,ful,gaa,kik,naq,kmb,mkw,luo,mfe,mos,nmq,ndc,snf,nde,sot,crs,nbl,ss,lua,twi,umb"
FORMAT="csv"
OUTPUT_FOLDER="${1:-/home/jonne/datasets/wikidata/final-resource-dump/}"
COMBO_OUTPUT="${OUTPUT_FOLDER}/final_resource.csv"
DEDUP_OUTPUT="${OUTPUT_FOLDER}/final_resource.dedup.csv"
MATRIX_OUTPUT="${OUTPUT_FOLDER}/final_resource.matrix.csv"

mkdir -p $OUTPUT_FOLDER

run () {

    CONLL_TYPE=$1
    OUTPUT="${OUTPUT_FOLDER}/${CONLL_TYPE}.csv"

    # dump everything into one file
    python wikidata_dump_transliterations.py \
        --strict \
        -t "${CONLL_TYPE}" \
        -l "${LANGS}" \
        -f "${FORMAT}" \
        -o - | tee "${OUTPUT}"

}

## first extract everything for each type
for conll_type in "PER" "LOC" "ORG"
do
    run $conll_type
done

# combine all the csvs into one big csv
cat $OUTPUT_FOLDER/PER.csv > $COMBO_OUTPUT
tail +2 $OUTPUT_FOLDER/LOC.csv >> $COMBO_OUTPUT
tail +2 $OUTPUT_FOLDER/ORG.csv >> $COMBO_OUTPUT

# deduplicate the rows by using "trumping rules" to break ties etc.
python deduplicate.py \
    -i $COMBO_OUTPUT \
    -o $DEDUP_OUTPUT

# finally create the matrix form
python create_matrix.py \
    -i $DEDUP_OUTPUT \
    -o $MATRIX_OUTPUT
