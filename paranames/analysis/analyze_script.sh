#!/usr/bin/env bash

set -euo pipefail

usage () {
    echo "Usage: bash ./script_analysis.sh TSV_PATH OUTPUT_FILE ALIAS_COL_IX LANG_COL_IX"
}

[ $# -lt 1 ] && usage && exit 1

analyze_most_common () {
    ## get all unique aliases
    tail +2 $DUMP | cut -f $ALIAS_COL_IX | sort | uniq > $ALIASES

    ## infer all scripts
    ./scripts/stat/infer_script_most_common < $ALIASES > $SCRIPTS

    ## combine into big flie
    paste $ALIASES $SCRIPTS > $TSV

    ## then use python script to analyze the script statistics, entropy etc
    python scripts/stat/get_language_stats.py \
        --input-file $DUMP \
        --cache-path $TSV \
        --output-file $OUTPUT_FILE
}

analyze_histogram () {

    ## first get the script histogram for each language
    for language in $LANGUAGES
    do
        script_histogram=$(tail +2 $DUMP |
            cut -f $ALIAS_COL_IX,$LANG_COL_IX  |
            grep -P "${language}$" |
            cut -f1 | 
            tr -d '\n' | 
            ./scripts/stat/infer_script_histogram --strip)
        printf "${language}\t${script_histogram}\n"

    done > $LANG_SCRIPTS

    ## get all unique aliases
    tail +2 $DUMP | cut -f $ALIAS_COL_IX | sort | uniq > $ALIASES

    ## infer all scripts
    ./scripts/stat/infer_script_histogram --strip < $ALIASES > $SCRIPTS
    
    ## combine into big file
    paste $ALIASES $SCRIPTS > $TSV

    ## then use python script to analyze the script statistics, entropy etc
    #python scripts/stat/tag_anomalous_names.py \
        #--input-file $DUMP \
        #--cache-path $TSV \
        #--language-script-histograms $LANG_SCRIPTS
        #--output-file $OUTPUT_FILE

}

DUMP=$1
OUTPUT_FILE=$2
ALIAS_COL_IX=${3:-3}
LANG_COL_IX=${4:-4}

## use fast command line tools to cache all aliases and scripts in a temp directory
TMPDIR=$(mktemp -d)
echo $TMPDIR
ALIASES=$TMPDIR/all_aliases.txt
SCRIPTS=$TMPDIR/all_aliases_scripts.txt
LANG_SCRIPTS=$TMPDIR/all_languages_script_histograms.tsv
TSV=$TMPDIR/all_aliases_with_script.tsv


## get unique languages
LANGUAGES=$(tail +2 $DUMP | cut -f $LANG_COL_IX | sort | uniq | tr '\n' ' ')

#analyze_most_common
analyze_histogram

#rm -rf $TMPDIR
