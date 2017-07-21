package com.github.gslowikowski.floow;

import java.io.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

    public static final String DEFAULT_MONGO_HOST = "localhost";
    public static final int DEFAULT_MONGO_PORT = 27017;
    public static final String DEFAULT_MONGO_DATABASE = "floow";
    public static final String DEFAULT_MONGO_COLLECTION = "words";

    public static final int DEFAULT_IMPORT_SKIP = 0;
    public static final int DEFAULT_IMPORT_LIMIT = 1000000;//TEMP 0;
    public static final int DEFAULT_CHUNK_SIZE = 100000;

    public static final String PARAM_HELP = "help";

    public static final String PARAM_SOURCE = "source";
    public static final String PARAM_ID = "id";
    public static final String PARAM_MONGO_ADDRESS = "mongo";
    public static final String PARAM_MONGO_DATABASE = "mongoDatabase";
    public static final String PARAM_MONGO_COLLECTION = "mongoCollection";

    public static final String PARAM_SKIP = "sourceLinesSkip";
    public static final String PARAM_LIMIT = "sourceLinesLimit";
    public static final String PARAM_CHUNK_SIZE = "chunk";

    public static final String PARAM_NOIMPORT = "noImport";
    public static final String PARAM_AGGREGATE = "aggregate";
    public static final String PARAM_MAPREDUCE = "mapReduce";

    public static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws IOException, ParseException {
        if (args.length == 0) {
            usage();
        }
        else {
            Options options = createOptions();
            CommandLine cmdLine = new DefaultParser().parse(options, args);

            if (cmdLine.hasOption(PARAM_HELP)) {
                usage();
            }
            else {
                String source = cmdLine.getOptionValue(PARAM_SOURCE);
                if (source == null) {
                    throw new MissingOptionException(PARAM_SOURCE);
                }

                String mongoHost = DEFAULT_MONGO_HOST;
                int mongoPort = DEFAULT_MONGO_PORT;
                if (cmdLine.hasOption(PARAM_MONGO_ADDRESS)) {
                    mongoHost = cmdLine.getOptionValue(PARAM_MONGO_ADDRESS);
                    if (mongoHost.contains(":")) {
                        String[] parts = mongoHost.split(":", 2);
                        mongoHost = parts[0];
                        try {
                            mongoPort = Integer.valueOf(parts[1]);
                        }
                        catch (NumberFormatException e) {
                            throw new ParseException(parts[1] + " is not a valid int value");
                        }
                    }
                }

                String mongoDatabase = cmdLine.getOptionValue(PARAM_MONGO_DATABASE, DEFAULT_MONGO_DATABASE);
                String mongoCollection = cmdLine.getOptionValue(PARAM_MONGO_COLLECTION, DEFAULT_MONGO_COLLECTION);

                boolean isNoImport = cmdLine.hasOption(PARAM_NOIMPORT);

                boolean isAggregate = cmdLine.hasOption(PARAM_AGGREGATE);
                String aggregateOutputCollection = isAggregate
                        ? cmdLine.getOptionValue(PARAM_AGGREGATE, mongoCollection + "_aggr") : null;

                boolean isMapReduce = cmdLine.hasOption(PARAM_MAPREDUCE);
                String mapReduceOutputCollection = isMapReduce
                        ? cmdLine.getOptionValue(PARAM_MAPREDUCE, mongoCollection + "_mr") : null;

                String serverId = cmdLine.getOptionValue(PARAM_ID);

                int importSkip = getIntOption(cmdLine, PARAM_SKIP, DEFAULT_IMPORT_SKIP);
                int importLimit = getIntOption(cmdLine, PARAM_LIMIT, DEFAULT_IMPORT_LIMIT);
                int chunkSize = getIntOption(cmdLine, PARAM_CHUNK_SIZE, DEFAULT_CHUNK_SIZE);

                MongoClient client = new MongoClient(mongoHost, mongoPort);
                try {
                    MongoDatabase db = client.getDatabase(mongoDatabase);

                    MongoCollection<Document> collection = db.getCollection(mongoCollection);

                    // Import only when -noimport parameter not specified
                    if (!isNoImport) {
                        File f = new File(source);
                        if (!f.exists()) {
                            throw new IllegalArgumentException("Source does not exist");
                        }
                        if (!f.isFile()) {
                            throw new IllegalArgumentException("Source is not a file");
                        }

                        Importer imp = new Importer(source, serverId, collection);
                        BufferedReader r = new BufferedReader(
                                new InputStreamReader(new FileInputStream(f), "UTF-8"));
                        try {
                            long startTs = System.currentTimeMillis();
                            imp.cleanData();
                            imp.importInChunks(r, importSkip, importLimit, chunkSize);
                            long endTs = System.currentTimeMillis();
                            log.info(String.format("Imported data in %d seconds.", (endTs-startTs)/1000));
                        }
                        finally {
                            r.close();
                        }
                    }

                    // Aggregate only if -aggregate or -mapReduce parameter specified
                    if (isAggregate || isMapReduce) {
                        Aggregator aggr = new Aggregator(collection, source);

                        if (isAggregate) {
                            long startTs = System.currentTimeMillis();
                            aggr.aggregate(aggregateOutputCollection);
                            long endTs = System.currentTimeMillis();
                            log.info(String.format("Aggregated results in %d seconds.", (endTs-startTs)/1000));
                        }

                        if (isMapReduce) {
                            long startTs = System.currentTimeMillis();
                            aggr.mapReduce(mapReduceOutputCollection);
                            long endTs = System.currentTimeMillis();
                            log.info(String.format("MapReduced results in %d seconds.", (endTs-startTs)/1000));
                        }
                    }
                }
                finally {
                    client.close();
                }
            }
        }
    }

    private static void usage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "java -jar challenge.jar", createOptions() );
    }

    protected static Options createOptions() {
        Options options = new Options()
        .addOption(
            Option.builder(PARAM_HELP) // help
                .build()
        )
        .addOption(
            Option.builder(PARAM_SOURCE) // source
                .desc("source file name")
                .hasArg()
                .argName("file_name")
                .build()
        )
        .addOption(
            Option.builder(PARAM_MONGO_ADDRESS) // mongo
                .desc(String.format("MongoDB server address (\"%s:%d\" by default)", DEFAULT_MONGO_HOST, DEFAULT_MONGO_PORT))
                .hasArg()
                .argName("host:port")
                .build()
        )
        .addOption(
            Option.builder(PARAM_MONGO_DATABASE) // mongoDatabase
                .desc(String.format("MongoDB database name (\"%s\" by default)", DEFAULT_MONGO_DATABASE))
                .hasArg()
                .argName("database_name")
                .build()
        )
        .addOption(
            Option.builder(PARAM_MONGO_COLLECTION) // mongoCollection
                .desc(String.format("MongoDB collection name (\"%s\" by default)", DEFAULT_MONGO_COLLECTION))
                .hasArg()
                .argName("collection_name")
                .build()
        )
        .addOption(
            Option.builder(PARAM_ID) // id
                .desc("server hostname/id string (null by default)")
                .hasArg()
                .build()
        )
        .addOption(
            Option.builder(PARAM_SKIP) // sourceLinesSkip
                .desc("number of lines in source file to skip (zero by default)")
                .hasArg()
                .type(Integer.class)
                .argName("skip_lines")
                .build()
        )
        .addOption(
            Option.builder(PARAM_LIMIT) // sourceLinesLimit
                .desc("number of lines in source file to import (unlimited by default)")
                .hasArg()
                .type(Integer.class)
                .argName("import_lines")
                .build()
        )
        .addOption(
            Option.builder(PARAM_CHUNK_SIZE) // chunk
                .desc("number of lines to preaggregate into single document (100000 by default)")
                .hasArg()
                .type(Integer.class)
                .argName("chunk_size")
                .build()
        )
        .addOption(
            Option.builder(PARAM_NOIMPORT) // noImport
                .desc(String.format("source is already imported, aggregate only (used with -%s or -%s)", PARAM_AGGREGATE, PARAM_MAPREDUCE))
                .build()
        )
        .addOption(
            Option.builder(PARAM_AGGREGATE) // aggregate
                .desc("after importing aggregate data to output collection using aggregation framework")
                .hasArg()
                .optionalArg(true)
                .argName("output_collection")
                .build()
        )
        .addOption(
            Option.builder(PARAM_MAPREDUCE) // mapReduce
                .desc("after importing aggregate data to output collection using mapReduce")
                .hasArg()
                .optionalArg(true)
                .argName("output_collection")
                .build()
        );

        return options;
    }

    private static int getIntOption(CommandLine cmdLine, String opt, int defaultValue) throws ParseException {
        int result = defaultValue;
        if (cmdLine.hasOption(opt)) {
            String strVal = cmdLine.getOptionValue(opt);
            try {
                result = Integer.valueOf(strVal).intValue();
            }
            catch (NumberFormatException e) {
                throw new ParseException(strVal + " is not a valid int value");
            }
        }
        return result;
    }

}
