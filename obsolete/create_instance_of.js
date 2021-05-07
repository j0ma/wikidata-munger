var instanceOfPipeline = [
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
        $out: "test_instance_of"
    }
];

db.wikidata_simple_preconference.aggregate(instanceOfPipeline, {allowDiskUse: true})
