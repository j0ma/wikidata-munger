var instanceOfPipeline = [
    //{$limit: 10000000},
    {$unwind: "$claims.P31"},
    {
        $addFields: {
            instance_of: "$claims.P31.mainsnak.datavalue.value.id"
        }
    },
    {
        $group: {
            _id: "$_id",
            "instance_of": {
                $push: "$instance_of"
            },
            "doc": { $first: '$$ROOT' }
        }
    },
    {
        $replaceRoot: {
            "newRoot": {
                "$mergeObjects": [
                    "$doc",
                    {instance_of: "$instance_of"}
                ]}
            }
    },
    {
        $out: "testingu"
        //$out: {
            //db: "wikidata_db", "coll": "test_instance_of"
        //}
    }
];

db.wikidata_simple_preconference.aggregate(instanceOfPipeline, {allowDiskUse: true})
