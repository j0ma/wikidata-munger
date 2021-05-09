#!/usr/bin/env bash

set -euo pipefail

usage () {
    echo "Usage: bash ./script_analysis.sh TSV_PATH"
}

[ $# -lt 1 ] && usage && exit 1

MATRIX=$1
COL_IX=${2:-3}

## use fast command line tools to cache all aliases and scripts in a temp directory
TMPDIR=$(mktemp -d)
ALIASES=$TMPDIR/all_aliases.txt
SCRIPTS=$TMPDIR/all_aliases_scripts.txt
TSV=$TMPDIR/all_aliases_with_script.tsv

cut -f $COL_IX $MATRIX | sort | uniq > $ALIASES
./scripts/stat/infer_script < $ALIASES > $SCRIPTS
paste $ALIASES $SCRIPTS > $TSV

echo $TMPDIR

## then use python script to analyze the script statistics, entropy etc
python scripts/stat/get_language_stats.py \
    --input-file $MATRIX \
    --cache-path $TSV

rm -rf --verbose $TMPDIR
