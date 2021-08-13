#!/usr/bin/env bash

set -euo pipefail

# Dumping everything, 2/5/2021

CONLL_TYPE="${1}"
LANGS="aa,af,ak,am,ny,ha,ig,rw,rn,kg,ln,lg,mg,nso,om,sn,so,sw,ti,ts,tn,ve,wo,xh,yo,zu,kea,ada,fon,ful,gaa,kik,naq,kmb,mkw,luo,mfe,mos,nmq,ndc,snf,nde,sot,crs,nbl,ss,lua,twi,umb"
FORMAT="${2}"
OUTPUT="/home/jonne/datasets/wikidata/african-language-dump-020521/${CONLL_TYPE}.csv" 

mkdir -p $(dirname ${OUTPUT})

# dump everything into one file
python paranames/io/wikidata_dump_transliterations.py \
    --strict \
    -t "${CONLL_TYPE}" \
    -l "${LANGS}" \
    -f "${FORMAT}" \
    -o - | tee "${OUTPUT}"

# separate by language
python paranames/io/separate_by_language.py \
    --lang-column "language" \
    --input-file "${OUTPUT}" 
