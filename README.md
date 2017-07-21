# Platform Engineer Challenge

`challenge.jar` can be found in `bin` directory.

### Usage

Just run:

```
java <jvm_parameters> -jar challenge.jar <parameters>
```

Running without parameters or with `-help` parameter shows usage information:

```
usage: java -jar challenge.jar
 -aggregate <output_collection>       after importing aggregate data to
                                      output collection using aggregation
                                      framework
 -chunk <chunk_size>                  number of lines to preaggregate into
                                      single document (100000 by default)
 -help
 -id <arg>                            server hostname/id string (null by
                                      default)
 -mapReduce <output_collection>       after importing aggregate data to
                                      output collection using mapReduce
 -mongo <host:port>                   MongoDB server address
                                      ("localhost:27017" by default)
 -mongoCollection <collection_name>   MongoDB collection name ("words" by
                                      default)
 -mongoDatabase <database_name>       MongoDB database name ("floow" by
                                      default)
 -noImport                            source is already imported,
                                      aggregate only (used with -aggregate
                                      or -mapReduce)
 -source <file_name>                  source file name
 -sourceLinesLimit <import_lines>     number of lines in source file to
                                      import (unlimited by default)
 -sourceLinesSkip <skip_lines>        number of lines in source file to
                                      skip (zero by default)
```

Document data processing is performed in two steps:

a) source file is processed and preaggregated word counts data are placed in a collection

b) preaggregated data are aggregated using one (or both) of the two aggregation methods: aggregation framework or mapReduce.

Both steps can be executed sequentially in one run, but in such case concurrent processing benefits will be lost.

### Source file processing and word counts data preaggregation.

Simplest call:

```
java -jar challenge.jar -source dump.txt
```

Whole `dump.txt` file is processed in one run.
Preaggregated word counts are stored in Mongo database available at `localhost:27017`.

Word counts are preaggregated in chunks of `100000` file lines. In other words for every
`100000` lines of the file, word counts are collected and then stored in one Mongo document 
in `words` collection inside `floow` database.

Preaggregating in Java saves time and reduces database size.

Database location, name and collection name or chunk size can be customized:

```
java -jar challenge.jar -source dump.txt -mongo host:port -mongoDatabase mydb -mongoCollection mycol -chunk 1000
```

### Concurrent source file processing and word counts data preaggregation.

In case of very large files users can process them concurrently. There are two ways possible:

#### Split the file into some parts.

Split large file into parts. Place the parts on different machines or, at least, in different directories.
All parts have to have the same file name (this is very important).
Import every part in separate process, giving it a unique `id`:

```
java -jar challenge.jar -source dump.txt -id uniqueId <other parameters>
```

#### Load parts of one same file.

Run multiple processes, loading different parts of the same file using `-sourceLinesSkip` 
and `-sourceLinesLimit` parameters. For example:

```
java -jar challenge.jar -source dump.txt -id uniqueId1 -sourceLinesLimit 1000000 <other parameters>
```
```
java -jar challenge.jar -source dump.txt -id uniqueId2 -sourceLinesSkip 1000000 -sourceLinesLimit 1000000 <other parameters>
```
```
java -jar challenge.jar -source dump.txt -id uniqueId3 -sourceLinesSkip 2000000 <other parameters>
```

First process loads only first one million rows. Second one skips those rows loaded by first process
and loads the second million. Third process loads the rest.

This option is simpler to use because it does not require splitting imput file into parts.
The input file must be available to all processes under the same name.


### Reloading in case of failures.

Before storing preaggregated data in a collection, old data for the same `-source` (and `-id`, if specified) is removed.

In case of problems with a single import process, rerun it with the same `-source` parameter value.

In case of problems with one of multiple import processes, rerun it with the same `-source`
and `-id` (and `-sourceLinesSkip` and `-sourceLinesLimit`, if they were specified) parameter values.


## Word counts aggregation

After processing a file and loading preaggregated data into Mongo database the second step 
is required - aggregation of the data.

Run:

```
java -jar challenge.jar -source dump.txt -noImport -aggregate
```

or

```
java -jar challenge.jar -source dump.txt -noImport -mapReduce
```

to aggregate word counts loaded from `dump.txt` file into separate collection using aggregation
framework of mapReduce.
To avoid importing the file again additional `-noImport` parameter is required.

`-aggregare` algorithm is faster, `-mapReduce` option is added mainly for comparison.

Because in this step only data already loaded into mongo database are processed,
source file does not have to be available anymore, only it's name is needed.

Both `-aggregate` and `-mapReduce` parameters can be specified in one run:

```
java -jar challenge.jar -source dump.txt -noImport -aggregate -mapReduce
```

Default output collection names are constructed by adding `_aggr` or `_mr`
to input collection name, e.g. if data were loaded to default `words` collection, the colections 
for aggregates will have names `words_aggr` and `words_mr` respectively.
Output collection names can be specified as parameter values, e.g.:

```
java -jar challenge.jar -source dump.txt -noImport -aggregate myaggrcol
```

or

```
java -jar challenge.jar -source dump.txt -noImport -mapReduce mymapreducecol
```

## All in one processing

Processing source file and aggregating word counts can be dome in one step. Concurrent processing
possibility is lost in this case.

Run:

```
java -jar challenge.jar -source dump.txt -aggregate
```

or

```
java -jar challenge.jar -source dump.txt -mapReduce
```

## Querying database for results

After processing source file and aggregating results we can query Mongo database for the final results
(word counts), e.g.:

```
db.words_aggr.find().sort({count:-1}).limit(3)
```

or

```
db.words_mr.find().sort({value:-1}).limit(3)
```
