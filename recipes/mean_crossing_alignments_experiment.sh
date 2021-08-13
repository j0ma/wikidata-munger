#!/usr/bin/env bash

set -euo pipefail

# Reproduces the mean crossing alignments experiments
# for the following name permuters:
# - no name permutation (baseline)
# - permute based on first comma
# - remove all parentheses
# - remove all parentheses, then permute
# - pick the best ordering of tokens based on edit_distance(english, uroman(non_english))

# NOTE: execute from root of repository

# TODO: change script path here once script is renamed
experiment_script="scripts/analysis/test_alignments.py"

# change these as needed
#input_dataset="$HOME/datasets/wikidata/per-all/combined/combined_dedup.tsv"
input_dataset="/home/jonne/datasets/wikidata/per-loc-org-everything/combined/combined_dedup.tsv"
chunksize=4000000
num_workers=16
debug_mode="false"  # change to "true" to enable
num_debug_chunks=1

current_date=$(date -u +"%m%d%y")
mca_experiment () {
    local permuter_type=${1:-baseline}
    local debug_mode=${2:-false}
    log_file="./log/mean_cross_alignments_${permuter_type}_${current_date}.log"
    names_folder="./data/name_reversal/permuted_names_by_model/${current_date}/${permuter_type}"

    if [ "${permuter_type}" = "baseline" ]
    then
        permute_flag=""
    else
        mkdir -p -v $names_folder
        permute_flag="--permute-tokens --permuter-type ${permuter_type}"
        permute_flag="${permute_flag} --write-permuted-names"
        permute_flag="${permute_flag} --names-output-folder ${names_folder}"
    fi

    if [ "${debug_mode}" = "true" ]
    then
        debug_flag="--debug-mode --num-debug-chunks ${num_debug_chunks}"
    else
        debug_flag=""
    fi

    
    python $experiment_script \
            -i $input_dataset \
            -lc "language" \
            --pool-languages \
            --chunksize "${chunksize}" \
            --num-workers "${num_workers}" \
            ${debug_flag} ${permute_flag} | tee ${log_file}
}

main () {
    permuters=(
        remove_parenthesis 
        remove_parenthesis_permute_comma 
        edit_distance
        baseline 
        comma 
    )
    for permuter in "${permuters[@]}"
    do
        echo "${permuter}"
        mca_experiment "${permuter}" "${debug_mode}"
    done
}

main
