package com.github.gslowikowski.floow;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Importer {

    public static final Logger log = LoggerFactory.getLogger(Importer.class);

    private String source;
    private String serverId;
    private MongoCollection<Document> collection;

    /**
     * Data importer.
     * 
     * @param source source file name
     * @param serverId additional identifier
     * @param collection collection storing imported data
     */
    public Importer(String source, String serverId, MongoCollection<Document> collection) {
        this.source = source;
        this.serverId = serverId;
        this.collection = collection;
    }

    /**
     * Cleans collection before import.
     * 
     * Removes only data related to the source (and additional identifier).
     */
    public void cleanData() {
        Document deleteFilter = new Document("source", source);
        if (serverId != null) {
            deleteFilter.append("id", serverId);
        }
        collection.deleteMany(deleteFilter);
    }

    /**
     * Imports data into collection.
     * 
     * @param r source buffered reader
     * @param skipLines number of lines to skip
     * @param limitLines number of lines to import
     * @param chunkSize aggregation chunk size
     * @throws IOException in case of I/O problems
     */
    public void importInChunks(BufferedReader r, int skipLines, int limitLines, int chunkSize) throws IOException {
        int lineCounter = 0;
        int chunkNo = 0;
        Map<String, Long> wordCounts = new HashMap<String, Long>();

        String line = r.readLine();

        if (skipLines > 0) {
            int skippedLineCounter = 0;
            while (line != null && skippedLineCounter < skipLines) {
                skippedLineCounter++;
                line = r.readLine();
            }
        }

        while (line != null && (limitLines == 0 || lineCounter < limitLines)) {
            String[] words = line.toLowerCase(Locale.UK).split("[^a-z]");
            for (String word: words) {
                if (word.length() > 0) {
                    Long count = wordCounts.get(word);
                    count = count != null ? Long.valueOf(count.longValue() + 1) : Long.valueOf(1L);
                    wordCounts.put(word, count);
                }
            }
            lineCounter++;
            if (chunkSize > 0 && lineCounter % chunkSize == 0) {
                log.debug(" saving chunk " + chunkNo);
                saveChunkDocument(source, serverId, chunkNo, chunkSize, collection, wordCounts);

                wordCounts = new HashMap<String, Long>(); // reset word map after saving a chunk
                chunkNo++;
            }
            line = r.readLine();
        }

        // save the rest
        if (!wordCounts.isEmpty()) {
            log.debug(".saving chunk " + chunkNo);
            saveChunkDocument(source, serverId, chunkNo, chunkSize, collection, wordCounts);
        }
    }

    private void saveChunkDocument(String source, String serverId, int chunkNo, int chunkSize,
            MongoCollection<Document> collection, Map<String, Long> wordCounts) {
        List<Document> wordsWithCounts = new ArrayList<Document>();
        for (Map.Entry<String, Long> e: wordCounts.entrySet()) {
            wordsWithCounts.add(new Document("word", e.getKey()).append("cnt", e.getValue().intValue()));
        }

        Document doc = new Document("source", source);
        if (serverId != null) {
            doc.append("id", serverId);
        }
        doc.append("chunkNo", chunkNo);
        doc.append("chunkSize", chunkSize);
        doc.append("words", wordsWithCounts);
        collection.insertOne(doc);
    }

}
