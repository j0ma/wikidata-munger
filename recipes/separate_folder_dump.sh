#!/usr/bin/env bash

set -euo pipefail

usage () {
    echo "Usage: bash separate_folder_dump.sh LANGUAGES OUTPUT_FOLDER [ENTITY_TYPES=PER,LOC,ORG]"
}

[ $# -lt 2 ] && usage && exit 1

langs="${1}"
format="tsv"
output_folder="${2}"
mkdir --verbose -p $output_folder/combined
entity_types=$(echo "${3}" | tr "," " ")

dump () {

    conll_type=$1
    output="${output_folder}/${conll_type}.tsv"

    # dump everything into one file
    python scripts/io/wikidata_dump_transliterations.py \
        --strict \
        -t "${conll_type}" \
        -l "${langs}" \
        -f "${format}" \
        -d "tab" \
        -o - | tee "${output}"

}

deduplicate () {
    local input_file=$1
    local dedup_file=$2

    # deduplicate the rows by using "trumping rules" to break ties etc.
    python scripts/io/deduplicate.py \
        -i $input_file \
        -o $dedup_file \
        -f tsv

}

separate_by_language () {
    
    local dedup_file=$1
    python scripts/io/separate_by_language.py \
        --input-file $dedup_file \
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

# extract & clean everything for each type
for conll_type in $entity_types
do
    dump $conll_type &
done
wait

# combine everything into one big tsv
combined_output="${output_folder}/combined.tsv"
echo "Combining everything to ${combined_output}"
#cat "${output_folder}/PER.tsv" >> $combined_output
#tail +2 "${output_folder}/LOC.tsv" >> $combined_output
#tail +2 "${output_folder}/ORG.tsv" >> $combined_output
csvstack --verbose --tabs ${output_folder}/*.tsv | csvformat -T >> $combined_output

# deduplicate combined tsv
dedup_output="${output_folder}/combined_dedup.tsv"
deduplicate $combined_output $dedup_output

# then separate into one tsv per entity type and language
for conll_type in $entity_types
do
    dedup_output_typed="${output_folder}/${conll_type}_dedup.tsv"
    separate_by_entity_type \
        "${dedup_output}" "${conll_type}" | 
        tee "${dedup_output_typed}" 

    separate_by_language "${dedup_output_typed}"
done

mv --verbose ${output_folder}/*.tsv ${output_folder}/combined
