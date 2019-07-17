package com.maxdemarzi.imports;

import com.maxdemarzi.results.StringResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class Imports {
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(name = "com.maxdemarzi.import", mode = Mode.WRITE)
    @Description("CALL com.maxdemarzi.import(file)")
    public Stream<StringResult> importData(@Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportDataRunnable(file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Data imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }
}
