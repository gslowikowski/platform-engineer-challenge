package com.github.gslowikowski.floow;

import java.util.Arrays;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

public class Aggregator {

    private MongoCollection<Document> collection;
    private String inputFileName;

    /**
     * Imported data aggregator.
     * 
     * @param collection input collection
     * @param inputFileName name of the file to aggregate data
     */
    public Aggregator(MongoCollection<Document> collection, String inputFileName) {
        this.collection = collection;
        this.inputFileName = inputFileName;
    }

    /**
     * Aggregates data using aggregation framework (faster).
     * 
     * db.words.aggregate(
     *   {$match: {'source': 'inputFileName'}},
     *   {$project: {'words': 1, '_id': 0}},
     *   {$unwind: '$words'},
     *   {$group: {'_id': '$words.word', 'count': {'$sum': '$words.cnt'}}},
     *   {$out: 'outputCollectionName'}
     * )
     * 
     * @param outputCollectionName collection for aggregated data
     */
    public void aggregate(String outputCollectionName) {
        collection.aggregate(Arrays.asList(
            new Document("$match", new Document("source", inputFileName)),
            new Document("$project", new Document("words", 1).append("_id", 0)),
            new Document("$unwind", "$words"),
            new Document("$group", new Document("_id", "$words.word").append("count", new Document("$sum", "$words.cnt"))),
            new Document("$out", outputCollectionName)
        )).toCollection();
    }

    /**
     * Aggregates data using mapReduce (slower).
     * 
     * @param outputCollectionName collection for aggregated data
     */
    public void mapReduce(String outputCollectionName) {
        String mapper = "function() { for (var i = 0, len = this.words.length; i < len; i++) { emit(this.words[i].word, this.words[i].cnt) } }";
        String reducer = "function(key, values) { return Array.sum(values) }";
        collection.mapReduce(mapper, reducer)
            .collectionName(outputCollectionName)
            .filter(new Document("source", inputFileName))
            //GS???.verbose(true)
            .toCollection();
    }

}
