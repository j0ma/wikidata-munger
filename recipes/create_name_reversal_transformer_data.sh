#!/usr/bin/env bash

set -euxo pipefail

# Creates training data for the name permutation transformer by first
# outputting all names with all permutations applied to them and then
# subsampling the desired number of rows.
# 
# Note: this sampling may either happen according to the empirical
# distribution of permuted names, or alternatively according to 50/50
# split of permuted & non-permuted names

# NOTE: execute from root of repository

# TODO: change script path here once script is renamed
create_data_script="scripts/analysis/permute_names_and_dump.py"
subsample_script="scripts/io/subsample.py"

# change these as needed
input_dataset="$HOME/datasets/wikidata/per-all/combined/combined_dedup.tsv"
combined_output="./data/name_reversal/train_data/transformer/combined_downsampled_5m_empirical_new.tsv"
chunksize=4000000
num_output_rows=5000000
num_workers=16
debug_mode="false"  # change to "true" to enable
num_debug_chunks=1
temporary_folder=$(mktemp -d)

# determine sampler parameters
sampler_smoothing_factor=0.7
sampler_type="exponential_smoothing"

create_data () {
    local permuter_type=${1:-baseline}
    local debug_mode=${2:-false}
    local data_output_folder=$3

    if [ "${debug_mode}" = "true" ]
    then
        debug_flag="--debug-mode --num-debug-chunks ${num_debug_chunks}"
    else
        debug_flag=""
    fi

    python "${create_data_script}" \
        -i  "${input_dataset}" \
        -lc "language" --permuter-type "${permuter_type}" \
        --chunksize "${chunksize}" --num-workers "${num_workers}"  \
        $debug_flag --names-output-folder "${data_output_folder}"
}


main () {
    local need_header="true"
    permuters=(
        remove_parenthesis 
        remove_parenthesis_permute_comma 
        edit_distance
        comma 
    )

    temp_combined_output="${temporary_folder}/combined.tsv"
    for permuter in "${permuters[@]}"
    do
        echo "${permuter}"

        # we need a folder to put name_permutations.tsv
        data_output_folder="${temporary_folder}/${permuter}"
        mkdir -v -p "${data_output_folder}"

        create_data "${permuter}" "${debug_mode}" "${data_output_folder}"
        
        # get header for combined output; this will only execute once 
        if [ "${need_header}" = "true" ]
        then
            echo "ANANASAKAAMA"
            head -1 ${data_output_folder}/*.tsv | tee -a "${temp_combined_output}"
            need_header="false"
        else
            # get the rest of the output and append to combined
            tail +2 ${data_output_folder}/*.tsv | tee -a "${temp_combined_output}"
        fi 

    done
    python "${subsample_script}" \
        -i "${temp_combined_output}" \
        -o "${combined_output}" -f tsv \
        -s balanced_groupby -g is_unchanged \
        -n "${num_output_rows}"
   

}

main
