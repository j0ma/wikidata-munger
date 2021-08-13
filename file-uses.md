## Files and their uses

- NOTE: [x] means it MUST be included in the final release

### Generic files / modules / classes
- [x] wikidata_helpers.py

### Insertion & database writing
- [x] bulk_insert_example.sh
    - Example call to wikidata_bulk_insert.py
- [x] wikidata_bulk_insert.py
    - Code for parallelized insert
- [x] create_index.py
    - Indexes a given field in a collection in a DB
- [x] wikidata_subclasses.py
    - Script to discover Wikidata entries that are subclasses of something
    - Writes to MongoDB or outputs to stdout
- create_instance_of.js
    - Finds instance-of information for each Wikidata entry
    - NOTE: moved to /obsolete/ as of 5/6/21
- wikidata_bulk_insert_old.py
    - Old non-parallelized code for insert
    - NOTE: moved to /obsolete/ as of 5/6/21
- insert_custom_metadata.py
    - Download -> skim -> Upsert
    - HORRIBLE pseudoparallel approach for inserting data
    - NOTE: moved to /obsolete/ as of 5/6/21
- wikidata_stream.py
    - Old insert that streams records from the bzip and inserts?
    - NOTE: moved to /obsolete/ as of 5/6/21

### Dumping artifacts from DB
- [x] wikidata_dump_transliterations.py
    - Main dumping script used by all the shell scripts
- count_documents_by_lang_id.py
    - Apparently counts documents per lang id
    - Uses pymongo
- per_lang_counts.py
    - Another script for computing per-lang counts?
    - Uses pymongo as well

### Web scraping
- [x] fetch_human_readable_lang_names.sh
    - Wraps around scrape_language_table.py
- [x] scrape_language_table.py
    - Scrapes a given language table from the web
    - Used to scrape the list of African languages, the list of Wikipedia language codes and other things

### Analysis of dumps
- get_most_common_scripts.sh
    - NOTE: moved to /obsolete/ as of 5/6/21
- compare_am_ti.py
    - Compares am/ti data
    - Used for google sheets
- [x] data_cleaning.py
    - Deduplicates by applying AfricaNLP trumping rules & Am/Ti filtering
- [x] create_matrix.py
    - Takes deduplicated dump data and turns it into matrix form
- get_lang_entropy_table.py
    - Analyzes coreutils-generated script counts and generates entropy for lang
    - NOTE: not specific to AfricaNLP
    - NOTE: obsolete for paranames
- get_lang_type_count_table.py
    - Counts per lang, mimics what I did in a notebook for AfricaNLP
    - NOTE: not specific to AfricaNLP
    - ./obsolete as of 5/16
- [x] script_based_analysis.py
    - Analyzes deduplicated dumps and computes script distributions/entropies
    - NOTE: not specific to AfricaNLP
    - NOTE: WORK IN PROGRESS
- [x] separate_by_language.py
    - Separates one dump into separate dumps separated by language

### Misc data munging
- [x] csv_to_latex.py
    - Converts a CSV table to a LaTeX table
