#!/usr/bin/env bash

python wikidata.py \
    -d /data-drive/datasets/wikidata/latest-all.json.bz2 \
    --instance-subclass-of Q5 \
    --insert-to-mongodb \
    --verbose
