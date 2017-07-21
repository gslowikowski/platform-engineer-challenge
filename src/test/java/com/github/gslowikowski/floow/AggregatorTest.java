package com.github.gslowikowski.floow;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AggregatorTest {

    private static final String SOURCE = "dump.txt";
    private static final String DATABASE = "floowtest";
    private static final String COLLECTION = "words";
    private static final String COLLECTION_AGGR = "words_aggr";
    private static final String COLLECTION_MR = "words_mr";
 
    private MongoClient client = null;
    private MongoDatabase db = null;
    private MongoCollection<Document> collection = null;

    @Before
    public void setUp() {
        client = new MongoClient();
        db = client.getDatabase(DATABASE);
        collection = db.getCollection(COLLECTION);
        collection.drop();
        collection.insertMany(getDocumentsToAggregate());
    }

    @After
    public void tearDown() {
        client.close();
        client = null;
    }

    @Test
    public void testAggregate() {
        Aggregator aggr = new Aggregator(collection, SOURCE);

        aggr.aggregate(COLLECTION_AGGR);

        List<Document> results = new ArrayList<>();
        db.getCollection(COLLECTION_AGGR).find().sort(new Document("count", -1).append("_id", 1)).into(results);

        assertEquals(7, results.size());
        assertEquals("awesome", results.get(0).get("_id"));
        assertEquals(4, results.get(0).get("count"));
        assertEquals("is", results.get(1).get("_id"));
        assertEquals(3, results.get(1).get("count"));
        assertEquals("mongo", results.get(2).get("_id"));
        assertEquals(2, results.get(2).get("count"));
        assertEquals("am", results.get(3).get("_id"));
        assertEquals(1, results.get(3).get("count"));
        assertEquals("floow", results.get(4).get("_id"));
        assertEquals(1, results.get(4).get("count"));
        assertEquals("i", results.get(5).get("_id"));
        assertEquals(1, results.get(5).get("count"));
        assertEquals("too", results.get(6).get("_id"));
        assertEquals(1, results.get(6).get("count"));
    }

    @Test
    public void testMapReduce() {
        Aggregator aggr = new Aggregator(collection, SOURCE);

        aggr.mapReduce(COLLECTION_MR);

        List<Document> results = new ArrayList<>();
        db.getCollection(COLLECTION_MR).find().sort(new Document("value", -1).append("_id", 1)).into(results);

        assertEquals(7, results.size());
        assertEquals("awesome", results.get(0).get("_id"));
        assertEquals(4.0, results.get(0).get("value"));
        assertEquals("is", results.get(1).get("_id"));
        assertEquals(3.0, results.get(1).get("value"));
        assertEquals("mongo", results.get(2).get("_id"));
        assertEquals(2.0, results.get(2).get("value"));
        assertEquals("am", results.get(3).get("_id"));
        assertEquals(1.0, results.get(3).get("value"));
        assertEquals("floow", results.get(4).get("_id"));
        assertEquals(1.0, results.get(4).get("value"));
        assertEquals("i", results.get(5).get("_id"));
        assertEquals(1.0, results.get(5).get("value"));
        assertEquals("too", results.get(6).get("_id"));
        assertEquals(1.0, results.get(6).get("value"));
    }

    private List<Document> getDocumentsToAggregate() {
        List<Document> result = new ArrayList<>();

        List<Document> words = new ArrayList<>();
        words.add(new Document("word", "mongo")
                .append("cnt", 2));
        words.add(new Document("word", "is")
                .append("cnt", 2));
        words.add(new Document("word", "awesome")
                .append("cnt", 2));
        Document doc = new Document("source", SOURCE)
                .append("chunkNo", 0)
                .append("chunkSize", 1)
                .append("words", words);
        result.add(doc);

        words = new ArrayList<>();
        words.add(new Document("word", "floow")
                .append("cnt", 1));
        words.add(new Document("word", "is")
                .append("cnt", 1));
        words.add(new Document("word", "awesome")
                .append("cnt", 1));
        doc = new Document("source", SOURCE)
                .append("chunkNo", 1)
                .append("chunkSize", 1)
                .append("words", words);
        result.add(doc);

        words = new ArrayList<>();
        words.add(new Document("word", "i")
                .append("cnt", 1));
        words.add(new Document("word", "am")
                .append("cnt", 1));
        words.add(new Document("word", "awesome")
                .append("cnt", 1));
        words.add(new Document("word", "too")
                .append("cnt", 1));
        doc = new Document("source", SOURCE)
                .append("chunkNo", 2)
                .append("chunkSize", 1)
                .append("words", words);
        result.add(doc);

        return result;
    }

}
