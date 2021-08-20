#!/usr/bin/env bash

set -euo pipefail

usage () {
    echo "Usage: bash separate_folder_dump.sh LANGUAGES OUTPUT_FOLDER [ENTITY_TYPES=PER,LOC,ORG DB_NAME=wikidata_db COLLECTION_NAME=wikidata_simple]"
}

[ $# -lt 2 ] && usage && exit 1

langs="${1}"
output_folder="${2}"
entity_types=$(echo "${3:-PER,LOC,ORG}" | tr "," " ")
db_name="${4:-wikidata_db}"
collection_name="${5:-wikidata_simple}"
format="tsv"
mkdir --verbose -p $output_folder/combined

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
        -f "${format}" \
        -d "tab" \
        --database-name "${db_name}" \
        --collection-name "${collection_name}" \
        -o - $exclude_langs_flag | tee "${output}"

}

filtering () {
    local input_file=$1
    local filtered_file=$2
    local permuter_type=$3

    # apply script filtering, entity name disambiguation etc.
    python paranames/io/filtering.py \
        -i $input_file -o $filtered_file -f tsv \
        --human-readable-langs-path "./data/human_readable_lang_names.json" \
        --permuter-type $permuter_type

}

separate_by_language () {
    
    local filtered_file=$1
    python paranames/io/separate_by_language.py \
        --input-file $filtered_file \
        --lang-column language \
        --io-format tsv \
        --use-subfolders \
        --verbose
}

separate_by_entity_type () {
    local input_file=$1
    local conll_type=$2
    local folder=$(dirname "${input_file}")
    local type_column=${3:-type}
    csvgrep \
        -v -t -c "${type_column}" \
        -r "${conll_type}" \
        < "${input_file}" |
    csvformat -T
}

echo "Extract & clean everything for each type"
for conll_type in $entity_types
do
    dump $conll_type $langs $db_name $collection_name &
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
    dumped_tsv="${output_folder}/${conll_type}.tsv"
    filtered_tsv="${output_folder}/${conll_type}_filtered.tsv"
    filtering $dumped_tsv $filtered_tsv $permuter_type
    separate_by_language "${filtered_tsv}"
done

echo "Combine everything into one big tsv"
combined_output="${output_folder}/combined_filtered.tsv"
echo "Combining everything to ${combined_output}"
csvstack --verbose --tabs ${output_folder}/*.tsv | csvformat -T | tee $combined_output

mv --verbose ${output_folder}/*.tsv ${output_folder}/combined
