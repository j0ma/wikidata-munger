from pymongo import MongoClient
from wikidata_helpers import orjson_dump


def main():
    raise NotImplementedError
    client = MongoClient()
    db = client["wikidata_db"]["wikidata_simple"]

    results = db.aggregate(
        [
            {"$project": {"languages": 1, "_id": 1}},
            {"$unwind": "$languages"},
            {"$group": {"_id": "$languages", "n_entities": {"$sum": 1}}},
            {"$project": {"language": "$_id", "n_entities": 1, "_id": 0}},
        ]
    )

    for result in results:
        print(orjson_dump(result))


if __name__ == "__main__":
    main()