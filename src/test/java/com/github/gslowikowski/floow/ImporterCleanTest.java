package com.github.gslowikowski.floow;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static org.junit.Assert.assertEquals;

public class ImporterCleanTest {

    private static final String DATABASE = "floowtest";
    private static final String COLLECTION = "words";

    private static final String SOURCE = "dump.txt";

    private MongoClient client = null;
    private MongoDatabase db = null;
    private MongoCollection<Document> collection = null;

    @Before
    public void setUp() {
        client = new MongoClient();
        db = client.getDatabase(DATABASE);
        collection = db.getCollection(COLLECTION);
        collection.drop();
        collection.insertMany(getInitialDocuments());
    }

    @After
    public void tearDown() {
        client.close();
        client = null;
    }

    @Test
    public void testCleanDataWithoutId() {
        Importer imp = new Importer(SOURCE, null, collection);

        imp.cleanData();

        List<Document> results = new ArrayList<>();
        db.getCollection(COLLECTION).find().into(results);

        assertEquals(1, results.size());
    }

    @Test
    public void testCleanDataWithId() {
        Importer imp = new Importer(SOURCE, "serverId1", collection);

        imp.cleanData();

        List<Document> results = new ArrayList<>();
        db.getCollection(COLLECTION).find().into(results);

        assertEquals(3, results.size());
    }

    private List<Document> getInitialDocuments() {
        List<Document> result = new ArrayList<>();

        List<Document> words = new ArrayList<>();
        words.add(new Document("word", "mongo")
                .append("cnt", 2));
        words.add(new Document("word", "is")
                .append("cnt", 2));
        words.add(new Document("word", "awesome")
                .append("cnt", 2));
        Document doc = new Document("source", "dump.txt")
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
        doc = new Document("source", "dump.txt")
                .append("id", "serverId1")
                .append("chunkNo", 1)
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
        doc = new Document("source", "dump.txt")
                .append("id", "serverId2")
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
        doc = new Document("source", "dump2.txt")
                .append("chunkNo", 2)
                .append("chunkSize", 1)
                .append("words", words);
        result.add(doc);

        return result;
    }

}
