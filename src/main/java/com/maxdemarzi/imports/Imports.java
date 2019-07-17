package com.maxdemarzi.imports;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.maxdemarzi.results.StringResult;
import com.maxdemarzi.schema.Labels;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.maxdemarzi.schema.Properties.NPI;

public class Imports {
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    private static GraphDatabaseService staticDB;

    static LoadingCache<String, Long> npis = Caffeine.newBuilder()
            .build(Imports::getNpis);

    static Long getNpis(String npi) {
        final Node node = staticDB.findNode(Labels.Provider, NPI, npi);
        if (node == null) { return -1L; }
        return node.getId();
    }

    @Procedure(name = "com.maxdemarzi.import", mode = Mode.WRITE)
    @Description("CALL com.maxdemarzi.import(reltype, file)")
    public Stream<StringResult> importData(@Name("reltype") String reltype, @Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        Thread t1 = new Thread(new ImportDataRunnable(reltype, file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Data imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }

    @Procedure(name = "com.maxdemarzi.import.shared", mode = Mode.WRITE)
    @Description("CALL com.maxdemarzi.import.shared(reltype, file)")
    public Stream<StringResult> importShared(@Name("reltype") String reltype, @Name("file") String file) throws InterruptedException {
        long start = System.nanoTime();

        if (staticDB == null) {
            staticDB = db;
        }

        Thread t1 = new Thread(new ImportSharedRunnable(reltype, file, db, log));
        t1.start();
        t1.join();

        return Stream.of(new StringResult("Data imported in " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds"));
    }
}
