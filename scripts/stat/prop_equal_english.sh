#!/usr/bin/env bash

set -euo pipefail

usage () {
    echo "Usage: ./prop_equal_english.sh DUMP_TSV"
}

[ $# -lt 1 ] && usage && exit 1

dump_file=${1}

csvcut -t $dump_file | 
    csvsql \
        --tables t \
        --query "select t.language, t.type, round(avg(t.eng == t.alias), 2) as equal from t group by t.language, t.type" | 
    csvformat -T
