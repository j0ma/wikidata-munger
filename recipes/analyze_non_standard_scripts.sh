#!/usr/bin/env bash

set -euo pipefail

data_folder=${1:-/home/jonne/datasets/wikidata/per-loc-org-script}

main() {
    local data_folder=$1
    for folder in "${data_folder}"/*-*
    do 
        echo $folder 
        for tsv_file in $folder/*.tsv
        do
            echo $tsv_file
            cut -f3 $tsv_file \
                | scripts/analysis/infer_script_most_common \
                | sort | uniq -c 
        done
    done
}
main $data_folder
