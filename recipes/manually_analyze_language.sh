#!/usr/bin/env bash

# Script for manual analysis of names in a given language
# Expected arguments: folder base path, a comma-separated list of languages.

# Default message to user in case arguments are not correct
usage () {
    echo "Usage: bash manually_analyze_language.sh BASE_FOLDER LANGUAGES"
}

[ $# -lt 2 ] && usage && exit 1

# Parse command line arguments
base_folder=$1
languages=$(echo $2 | tr ',' ' ')

# Edit this to adjust paranames prefix
paranames_prefix=$HOME/paranames

# Script inference helper script
infer_most_common=$paranames_prefix/paranames/analysis/infer_script_most_common

# Script histogram for type in lang
for lang in $languages
do
    echo "Language: ${lang}"
    for conll_type in PER LOC ORG
    do
        echo "Type: ${conll_type}"
        cut -f3 ${base_folder}/${lang}/${conll_type}*.tsv \
            | $infer_most_common \
            | sort | uniq -c
        echo
    done
    echo
done
