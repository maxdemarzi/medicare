package com.maxdemarzi.schema;

import com.maxdemarzi.results.StringResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.util.stream.Stream;

import static com.maxdemarzi.schema.Properties.NAME;
import static com.maxdemarzi.schema.Properties.NPI;

public class Schema {

    @Context
    public GraphDatabaseService db;


    @Procedure(name = "com.maxdemarzi.schema.generate", mode = Mode.SCHEMA)
    @Description("CALL com.maxdemarzi.schema.generate() - generate schema")

    public Stream<StringResult> generate() throws IOException {
        org.neo4j.graphdb.schema.Schema schema = db.schema();

        if (!schema.getConstraints(Labels.Drug).iterator().hasNext()) {
            schema.constraintFor(Labels.Drug)
                    .assertPropertyIsUnique(NAME)
                    .create();
        }

        if (!schema.getConstraints(Labels.Specialty).iterator().hasNext()) {
            schema.constraintFor(Labels.Specialty)
                    .assertPropertyIsUnique(NAME)
                    .create();
        }

        if (!schema.getConstraints(Labels.Generic).iterator().hasNext()) {
            schema.constraintFor(Labels.Generic)
                    .assertPropertyIsUnique(NAME)
                    .create();
        }

        if (!schema.getConstraints(Labels.Location).iterator().hasNext()) {
            schema.constraintFor(Labels.Location)
                    .assertPropertyIsUnique(NAME)
                    .create();
        }

        if (!schema.getConstraints(Labels.Provider).iterator().hasNext()) {
            schema.constraintFor(Labels.Provider)
                    .assertPropertyIsUnique(NPI)
                    .create();
        }


        return Stream.of(new StringResult("Schema Generated"));
    }

}