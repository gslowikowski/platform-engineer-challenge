package com.github.gslowikowski.floow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ImporterTest {

    private static final String DATABASE = "floowtest";
    private static final String COLLECTION = "words";

    private static final String SOURCE = "dump.txt";
    private static final int CHUNK_SIZE = 2;

    private MongoClient client = null;
    private MongoDatabase db = null;
    private MongoCollection<Document> collection = null;

    @Before
    public void setUp() {
        client = new MongoClient();
        db = client.getDatabase(DATABASE);
        collection = db.getCollection(COLLECTION);
        collection.drop();
    }

    @After
    public void tearDown() {
        client.close();
        client = null;
    }

    @Test
    public void testImport() throws IOException {
        Importer imp = new Importer(SOURCE, null/*id*/, collection);

        BufferedReader r = new BufferedReader(
                new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(SOURCE)));
        try {
            imp.cleanData();
            imp.importInChunks(r, 0/*skipLines*/, 0/*limitLines*/, CHUNK_SIZE);
        }
        finally {
            r.close();
        }

        List<Document> results = new ArrayList<>();
        db.getCollection(COLLECTION).find().into(results);

        assertEquals(2, results.size());

        assertEquals("dump.txt", results.get(0).get("source"));
        assertEquals(0, results.get(0).get("chunkNo"));
        assertEquals(2, results.get(0).get("chunkSize"));
        assertTrue(results.get(0).get("words") instanceof List);
        List<Document> words = (List<Document>)results.get(0).get("words");
        assertTrue(words.contains(new Document("word", "mongo").append("cnt", 2)));
        assertTrue(words.contains(new Document("word", "floow").append("cnt", 1)));
        assertTrue(words.contains(new Document("word", "is").append("cnt", 3)));
        assertTrue(words.contains(new Document("word", "awesome").append("cnt", 3)));

        assertEquals("dump.txt", results.get(1).get("source"));
        assertEquals(1, results.get(1).get("chunkNo"));
        assertEquals(2, results.get(1).get("chunkSize"));
        assertTrue(results.get(1).get("words") instanceof List);
        words = (List<Document>)results.get(1).get("words");
        assertTrue(words.contains(new Document("word", "i").append("cnt", 1)));
        assertTrue(words.contains(new Document("word", "am").append("cnt", 1)));
        assertTrue(words.contains(new Document("word", "awesome").append("cnt", 1)));
        assertTrue(words.contains(new Document("word", "too").append("cnt", 1)));
    }

    @Test
    public void testImportWithSkipAndLimit() throws IOException {
        Importer imp = new Importer(SOURCE, "serverId1"/*id*/, collection);

        BufferedReader r = new BufferedReader(
                new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(SOURCE)));
        try {
            imp.cleanData();
            imp.importInChunks(r, 1/*skipLines*/, 1/*limitLines*/, CHUNK_SIZE);
        }
        finally {
            r.close();
        }

        List<Document> results = new ArrayList<>();
        db.getCollection(COLLECTION).find().into(results);

        assertEquals(1, results.size());

        assertEquals("dump.txt", results.get(0).get("source"));
        assertEquals(0, results.get(0).get("chunkNo"));
        assertEquals(2, results.get(0).get("chunkSize"));
        assertTrue(results.get(0).get("words") instanceof List);
        List<Document> words = (List<Document>)results.get(0).get("words");
        assertTrue(words.contains(new Document("word", "floow").append("cnt", 1)));
        assertTrue(words.contains(new Document("word", "is").append("cnt", 1)));
        assertTrue(words.contains(new Document("word", "awesome").append("cnt", 1)));
    }

}
