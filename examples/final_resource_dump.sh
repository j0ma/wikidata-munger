#!/usr/bin/env bash

set -euo pipefail

LANGS="aa,af,ak,am,ny,ha,ig,rw,rn,kg,ln,lg,mg,nso,om,sn,so,sw,ti,ts,tn,ve,wo,xh,yo,zu,kea,ada,fon,ful,gaa,kik,naq,kmb,mkw,luo,mfe,mos,nmq,ndc,snf,nde,sot,crs,nbl,ss,lua,twi,umb"
FORMAT="tsv"
OUTPUT_FOLDER="${1:-/home/jonne/datasets/wikidata/final-resource-dump/}"
COMBO_OUTPUT="${OUTPUT_FOLDER}/final_resource.tsv"
DEDUP_OUTPUT="${OUTPUT_FOLDER}/final_resource.dedup.tsv"
MATRIX_OUTPUT="${OUTPUT_FOLDER}/final_resource.matrix.tsv"

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
    -o $DEDUP_OUTPUT \
    -f tsv

# finally create the matrix form
python create_matrix.py \
    -i $DEDUP_OUTPUT \
    -o $MATRIX_OUTPUT 
