#!/usr/bin/env bash

## Deploy script for Wikidata

set -euo pipefail

## Constant
IO_SCRIPT_FOLDER=scripts/io

## Command line arguments
INPUT_JSON=${1}
DB_NAME=${2:-wikidata}
COLL_NAME=${3:-wikidata_simple}
DEFAULT_CPUS=$(nproc)
N_WORKERS=${4:-$DEFAULT_CPUS}
CHUNKSIZE=${5:-5000}

# ingest into mongo db
#python $IO_SCRIPT_FOLDER/wikidata_bulk_insert.py \
    #-d "${INPUT_JSON}" \
    #--database-name "${DB_NAME}" \
    #--collection-name "${COLL_NAME}" \
    #-w ${N_WORKERS} -c ${CHUNKSIZE} \
    #--simple-records --debug

# create indices
for field in "instance_of" "languages" "id" "name"
do
    python $IO_SCRIPT_FOLDER/create_index.py -db "${DB_NAME}" -c "${COLL_NAME}" -f "${field}"
done

# create "subclasses" collection
python $IO_SCRIPT_FOLDER/wikidata_subclasses.py \
    --entity-ids "Q43229,Q5,Q82794" \
    -db ${DB_NAME} -c subclasses
