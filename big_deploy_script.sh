#!/usr/bin/env bash

## Deploy script for Wikidata

set -euo pipefail

## Command line arguments
INPUT_JSON=${1}
DB_NAME=${2:-wikidata}
COLL_NAME=${3:-wikidata_simple}
DEFAULT_CPUS=$(nproc)
N_WORKERS=${4:-$DEFAULT_CPUS}
CHUNKSIZE=${5:-5000}

# ingest into mongo db
#python wikidata_bulk_insert_decompressed.py \
    #-d "${INPUT_JSON}" \
    #--database-name "${DB_NAME}" \
    #--collection-name "${COLL_NAME}" \
    #-w ${N_WORKERS} -c ${CHUNKSIZE} \
    #--simple-records --debug

# create indices
for field in "instance_of" "languages" "id" "name"
do
    python create_index.py -db "${DB_NAME}" -c "${COLL_NAME}" -f "${field}"
done

# create "subclasses" collection
python wikidata_subclasses.py \
    --entity-ids "Q43229,Q5,Q82794" \
    -db ${DB_NAME} -c subclasses
