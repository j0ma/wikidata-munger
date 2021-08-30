python paranames/scrape/scrape_language_table.py \
    -u https://www.wikidata.org/wiki/Help:Wikimedia_language_codes/lists/all \
    -s .wikitable -c a,b,c,d,e,f,g,h,i,j --lang-col a --value-col e | jq
