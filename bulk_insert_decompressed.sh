python wikidata_bulk_insert_decompressed.py \
    -d /mnt/storage/data/wikidata/latest-all.json \
    --database-name wikidata_db \
    --collection-name wikidata_simple2 \
    -w 16 -c 5000 --simple-records --debug
