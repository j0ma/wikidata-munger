usage () {
    echo "Usage: ./counts_per_lang_and_type.sh TSV_PATH"
}

[ $# -lt 1 ] && usage && exit 1

tsv_file=$1

tail +2 $tsv_file | cut -f4,5 | sort | uniq -c | sort -nr
