#!/usr/bin/env bash

set -euo pipefail

usage () {
    echo "Usage: bash separate_folder_dump.sh LANGUAGES OUTPUT_FOLDER [ENTITY_TYPES=PER,LOC,ORG DB_NAME=wikidata_db COLLECTION_NAME=wikidata_simple VOTING_METHOD=majority_vote NUM_WORKERS=n_cpus]"
}

[ $# -lt 4 ] && usage && exit 1

langs="${1}"
output_folder="${2}"
entity_types=$(echo "${3:-PER,LOC,ORG}" | tr "," " ")
db_name="${4:-wikidata_db}"
collection_name="${5:-wikidata_simple}"
default_format="tsv"
voting_method=${6:-majority_vote}

default_num_workers=$(nproc)
num_workers=${7:-$default_num_workers}

extra_data_folder="${output_folder}"/extra_data

mkdir --verbose -p $output_folder/combined
mkdir --verbose -p $extra_data_folder

# NOTE: edit this to increase/decrease threshold for excluding small languages
default_name_threshold=1000

# NOTE: add comma separted list here to exclude languages
exclude_these_langs=""

dump () {

    local conll_type=$1
    local langs=$2
    local db_name=$3
    local collection_name=$4
    local output="${output_folder}/${conll_type}.tsv"

    if [ "${langs}" = "all" ]
    then
        langs_flag=""
        strict_flag=""
    else
        langs_flag="-l ${langs}"
        strict_flag="--strict"
    fi

    if [ -z "${exclude_these_langs}" ]
    then
        echo "[INFO] No languages being excluded."
        #exclude_langs_flag="-L en"
        exclude_langs_flag=""
    else
        echo "[INFO] Excluding ${exclude_these_langs//,/, }."
        exclude_langs_flag="-L ${exclude_these_langs}"
    fi

    # dump everything into one file
    python paranames/io/wikidata_dump_transliterations.py \
        $strict_flag \
        -t "${conll_type}" $langs_flag \
        -f "${default_format}" \
        -d "tab" \
        --database-name "${db_name}" \
        --collection-name "${collection_name}" \
        -o - $exclude_langs_flag > "${output}"

}

postprocess () {
    local input_file=$1
    local output_file=$2

    # apply things like entity name disambiguation rules
    python paranames/io/postprocess.py \
        -i $input_file -o $output_file -f $default_format -m $default_name_threshold

}

standardize_script () {
    local input_file=$1
    local output_file=$2
    local vote_aggregation_method=$3
    local num_workers=$4

    # apply script standardization
    python paranames/io/script_standardization.py \
        -i $input_file -o $output_file -f tsv \
        --vote-aggregation-method $vote_aggregation_method \
        --filtered-names-output-file "${extra_data_folder}/filtered_names.tsv" \
        --write-filtered-names --compute-script-entropy --num-workers $num_workers
}

standardize_names () {
    local input_file=$1
    local output_file=$2
    local permuter_type=$3
    local conll_type=$4

    # apply script standardization
    python paranames/io/name_standardization.py \
        -i $input_file -o $output_file -f tsv \
        --human-readable-langs-path ~/paranames/data/human_readable_lang_names.json \
        --permuter-type $permuter_type --corpus-stats-output ${extra_data_folder}/standardize_names_stats_$conll_type \
        --debug-mode --num-workers $num_workers --corpus-require-english
}

compute_script_entropy () {
    local input_file=$1
    local output_file=$2
    local script="paranames/analysis/script_entropy_with_cache.sh"
    bash $script $input_file $output_file
}

separate_by_language () {
    
    local filtered_file=$1
    python paranames/io/separate_by_language.py \
        --input-file $filtered_file \
        --lang-column language \
        --io-format $default_format \
        --use-subfolders \
        --verbose
}


csv2tsv () {
    xsv fmt -t"\t"
}

combine_tsv_files () {
    local glob="$@"
    xsv cat rows -d"\t" ${glob} | csv2tsv
}

separate_by_entity_type () {
    local input_file=$1
    local conll_type=$2
    local folder=$(dirname "${input_file}")
    local type_column=${3:-type}
    xsv search -d"\t" -s "${type_column}" "${conll_type}" < "${input_file}" | csv2tsv
}

echo "Extract & clean everything for each type"
for conll_type in $entity_types
do
    dump $conll_type $langs $db_name $collection_name &
done
wait

# combine everything into one tsv for script standardization
# this way we get interpretable entropy numbers by language, not just
combined_tsv="${output_folder}/combined_postprocessed.tsv"
combine_tsv_files ${output_folder}/*.tsv > $combined_tsv

combined_postprocessed_tsv="${output_folder}/combined_postprocessed.tsv"
postprocess $combined_tsv $combined_postprocessed_tsv

# compute script entropy (before)
script_entropy_results_before="${extra_data_folder}/tacl_script_entropy_${voting_method}_before.tsv"
compute_script_entropy $combined_postprocessed_tsv $script_entropy_results_before

# script standardization: remove parentheses from everything
combined_script_standardized_tsv="${output_folder}/combined_script_standardized_${voting_method}.tsv"
standardize_script \
    $combined_postprocessed_tsv \
    $combined_script_standardized_tsv \
    $voting_method $num_workers

# compute script entropy (after)
script_entropy_results_after="${extra_data_folder}/tacl_script_entropy_${voting_method}_after.tsv"
compute_script_entropy $combined_script_standardized_tsv $script_entropy_results_after

# separate into PER,LOC,ORG for name permutations
for conll_type in $entity_types
do
    separate_by_entity_type $combined_script_standardized_tsv $conll_type \
        > "${output_folder}/${conll_type}_script_standardized_${voting_method}.tsv" &
done
wait

echo "Filter dumped TSVs for each entity type..."
for conll_type in $entity_types
do
    if [ "$conll_type" = "PER" ]
    then
        permuter_type="remove_parenthesis_edit_distance"
    else
        permuter_type="remove_parenthesis"
    fi
    echo "Type: ${conll_type}	Permuter: ${permuter_type}"
    name_standardization_input_tsv="${output_folder}/${conll_type}_script_standardized_${voting_method}.tsv"
    name_standardization_output_tsv="${output_folder}/${conll_type}_script_name_standardized_${voting_method}.tsv"
    standardize_names $name_standardization_input_tsv $name_standardization_output_tsv $permuter_type $conll_type
done

echo "Combine everything into one big tsv"
final_combined_output="${output_folder}/combined_script_name_standardized_${voting_method}.tsv"
echo "Destination: ${final_combined_output}"

final_combination_entity_types=$(echo $entity_types | tr " " ",")
combine_tsv_files ${output_folder}/*_script_name_standardized_${voting_method}.tsv > $final_combined_output

separate_by_language $final_combined_output

mv --verbose ${output_folder}/{PER,LOC,ORG,combined}*.tsv ${output_folder}/combined
