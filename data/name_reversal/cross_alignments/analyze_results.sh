#!/usr/bin/env bash

set -euo pipefail

folder=$1

csvjoin -t -c 'language_long' $folder/*.tsv #| tabview -
