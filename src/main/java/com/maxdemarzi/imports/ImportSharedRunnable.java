package com.maxdemarzi.imports;

import de.siegmar.fastcsv.reader.*;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.maxdemarzi.imports.Imports.npis;
import static com.maxdemarzi.schema.Properties.*;

public class ImportSharedRunnable implements Runnable{

    private static final int TRANSACTION_LIMIT = 10000;
    private String filename;
    private GraphDatabaseService db;
    private Log log;
    private RelationshipType sharedType;

    public ImportSharedRunnable(String reltype, String filename, GraphDatabaseService db, Log log) {
        this.filename = filename;
        this.db = db;
        this.log = log;
        this.sharedType = RelationshipType.withName(reltype);
    }

    @Override
    public void run() {
        File file = new File(filename);
        CsvReader csvReader = new CsvReader();
        csvReader.setContainsHeader(false);
        csvReader.setFieldSeparator(',');

        Transaction tx = db.beginTx();
        try {
            int count = 0;
            CsvParser csvParser = csvReader.parse(file, StandardCharsets.UTF_8);
            CsvRow row;
            while ((row = csvParser.nextRow()) != null) {
                Long from = npis.get(row.getField(0));
                Long to = npis.get(row.getField(1));

                // Skip NPIs that are not found
                if (from == null || from == -1L) {
                    continue;
                }

                if (to == null || to == -1L) {
                    continue;
                }

                count++;
                Relationship shared = db.getNodeById(from).createRelationshipTo(db.getNodeById(to), sharedType);
                shared.setProperty(PAIR_COUNT, coalesceLong(row.getField(2)));
                shared.setProperty(BENE_COUNT, coalesceLong(row.getField(3)));
                shared.setProperty(SAMEDAY_COUNT, coalesceLong(row.getField(4)));

                // Commit to Neo4j and Start a new Transaction
                if (count % TRANSACTION_LIMIT == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }
            tx.success();
        } catch (Exception exception) {
            log.error("Error found in Importing Shared:");
            log.error(Arrays.stream(exception.getStackTrace())
                    .map(Objects::toString)
                    .collect(Collectors.joining("\n")));
        } finally {
            tx.close();
        }
    }

    private Long coalesceLong(String text) {
        if (StringUtils.isNumeric(text.trim())) {
            return Long.valueOf(text.trim());
        }
        return 0L;
    }
}
