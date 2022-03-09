#!/usr/bin/env bash

# NOTE: the use of "rg" with a relatively simple regex can cause other fields besides the language field
# to be matched as well if another field has a value equal to the language in question

input_tsv=$1
output_tsv=${2:-subsampled.tsv}
n_per_lang=${3:-10000}
language_column=${4:-language}
temp_output_folder=$(mktemp -d subsampled_XXXXX -p /tmp)

echo "Temporary output folder: ${temp_output_folder}"

echo "Getting unique languages..."
n_lines=$(wc -l $input_tsv | cut -f1 -d' ')
uniq_langs=$(xsv select $language_column $input_tsv | tqdm --total=$n_lines | tail +2 | sort | uniq | tr '\n' ' ')

echo "Subsampling..."
for lang in $uniq_langs
do 
    xsv search -s language "^${lang}$" $input_tsv \
        | xsv sample -n $n_per_lang > $temp_output_folder/$lang.sub &
done 

wait

echo "Concatenating..."
echo "Removing old output TSV if it exists"
rm -vf $output_tsv
xsv cat rows ${temp_output_folder}/*.sub | xsv fmt -t "\t" >> ${output_tsv}
#rm -vrf ${temp_output_folder}
