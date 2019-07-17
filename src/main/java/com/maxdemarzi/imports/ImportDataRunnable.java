package com.maxdemarzi.imports;

import com.maxdemarzi.schema.Labels;
import com.maxdemarzi.schema.RelationshipTypes;
import de.siegmar.fastcsv.reader.*;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static com.maxdemarzi.schema.Properties.*;

public class ImportDataRunnable implements Runnable{

    private static final int TRANSACTION_LIMIT = 10000;
    private String filename;
    private GraphDatabaseService db;
    private Log log;
    private RelationshipType prescribedType;

    public ImportDataRunnable(String reltype, String filename, GraphDatabaseService db, Log log) {
        this.filename = filename;
        this.db = db;
        this.log = log;
        this.prescribedType = RelationshipType.withName(reltype);
    }

    @Override
    public void run() {
        File file = new File(filename);
        CsvReader csvReader = new CsvReader();
        csvReader.setContainsHeader(true);
        csvReader.setFieldSeparator('\t');

        // Node Cache
        HashMap<String, Node> drugs = new HashMap<>();
        HashMap<String, Node> specialties = new HashMap<>();
        HashMap<String, Node> generics = new HashMap<>();
        HashMap<String, Node> locations = new HashMap<>();
        HashMap<String, Node> npis = new HashMap<>();
        HashMap<String, HashMap<String, Object>> npiProperties = new HashMap<>();

        // Warm up Cache
        try(Transaction tx = db.beginTx();) {
            ResourceIterator<Node> iterator = db.findNodes(Labels.Drug);
            while(iterator.hasNext()) {
                Node node = iterator.next();
                drugs.put((String)node.getProperty(NAME), node);
            }

            iterator = db.findNodes(Labels.Specialty);
            while(iterator.hasNext()) {
                Node node = iterator.next();
                specialties.put((String)node.getProperty(NAME), node);
            }

            iterator = db.findNodes(Labels.Generic);
            while(iterator.hasNext()) {
                Node node = iterator.next();
                generics.put((String)node.getProperty(NAME), node);
            }

            iterator = db.findNodes(Labels.Location);
            while(iterator.hasNext()) {
                Node node = iterator.next();
                locations.put((String)node.getProperty(NAME), node);
            }
            iterator = db.findNodes(Labels.Provider);
            while(iterator.hasNext()) {
                Node node = iterator.next();
                npis.put((String)node.getProperty(NPI), node);
            }
        }

        // Relationship Cache
        HashMap<String, Relationship> globalRelationships = new HashMap<>();
        HashMap<String, Relationship> providerRelationships = new HashMap<>();

        // We're going to make two passes at the file.
        // First pass we will just do nodes
        try (CsvParser csvParser = csvReader.parse(file, StandardCharsets.UTF_8)) {
            CsvRow row;
            while ((row = csvParser.nextRow()) != null) {
                drugs.putIfAbsent(row.getField("drug_name"), null);
                specialties.putIfAbsent(row.getField("specialty_description"), null);
                generics.putIfAbsent(row.getField("generic_name"), null);
                locations.putIfAbsent(row.getField("nppes_provider_city") + ", " + row.getField("nppes_provider_state"), null);
                String npi = row.getField(NPI);
                if (!npis.containsKey(npi)) {
                    npis.put(npi, null);
                    if (!npiProperties.containsKey(npi)) {
                        HashMap<String, Object> properties = new HashMap<>();
                        properties.put(FIRST_NAME, row.getField("nppes_provider_first_name"));
                        properties.put(LAST_NAME, row.getField("nppes_provider_last_org_name"));
                        npiProperties.put(npi, properties);
                    }
                }
            }
        } catch (IOException exception) {
            log.error("Error found in reading data:");
            log.error(Arrays.stream(exception.getStackTrace())
                    .map(Objects::toString)
                    .collect(Collectors.joining("\n")));
        }

        importNodes(drugs, Labels.Drug);
        importNodes(specialties, Labels.Specialty);
        importNodes(generics, Labels.Generic);
        importNodes(locations, Labels.Location);

        // Import NPIs
        Transaction tx = db.beginTx();
        try {
            int count = 0;

            for (Map.Entry<String, Node> entry : npis.entrySet()) {
                if (entry.getValue() == null) {
                    Node node = db.findNode(Labels.Provider, NPI, entry.getKey());
                    if (node == null) {
                        node = db.createNode(Labels.Provider);
                        node.setProperty(NPI, entry.getKey());
                        HashMap<String, Object> properties = npiProperties.get(entry.getKey());
                        node.setProperty(FIRST_NAME, properties.get(FIRST_NAME));
                        node.setProperty(LAST_NAME, properties.get(LAST_NAME));
                        count++;

                        // Commit to Neo4j and Start a new Transaction
                        if (count % TRANSACTION_LIMIT == 0) {
                            tx.success();
                            tx.close();
                            tx = db.beginTx();
                        }
                    }
                    npis.put(entry.getKey(), node);
                }

            }

            tx.success();
        } catch (Exception exception) {
            log.error("Error found in Importing Data - Nodes:");
            log.error(Arrays.stream(exception.getStackTrace())
                    .map(Objects::toString)
                    .collect(Collectors.joining("\n")));
        } finally {
            tx.close();
        }

        // Second pass for Relationships
        int count = 0;
        tx = db.beginTx();
        try {
            try (CsvParser csvParser = csvReader.parse(file, StandardCharsets.UTF_8)) {
                CsvRow row;
                String lastNPI = "";
                Node provider = null;
                while ((row = csvParser.nextRow()) != null) {
                    count++;
                    // Get the nodes
                    Node drug = drugs.get(row.getField("drug_name"));
                    Node generic = generics.get(row.getField("generic_name"));
                    Node specialty = specialties.get(row.getField("specialty_description"));
                    Node location = locations.get(row.getField("nppes_provider_city") + ", " + row.getField("nppes_provider_state"));

                    // First Global Relationships
                    createRelationship(globalRelationships, drug, generic, RelationshipTypes.HAS_GENERIC);

                    // Then Provider specific Relationships
                    String currentNPI = row.getField(NPI);
                    if (!lastNPI.equals(currentNPI)) {
                        providerRelationships = new HashMap<>();
                        provider = npis.get(currentNPI);
                        lastNPI = currentNPI;
                        if (provider == null) {
                            continue;
                        }
                    }

                    createRelationship(providerRelationships, provider, specialty, RelationshipTypes.HAS_SPECIALTY);
                    createRelationship(providerRelationships, provider, location, RelationshipTypes.IN_LOCATION);
                    if (drug == null) {
                        continue;
                    }
                    Relationship prescribed = provider.createRelationshipTo(drug, prescribedType);

                    prescribed.setProperty(BENE_COUNT, coalesceLong(row.getField(BENE_COUNT)));
                    prescribed.setProperty(TOTAL_CLAIM_COUNT, coalesceLong(row.getField(TOTAL_CLAIM_COUNT)));
                    prescribed.setProperty(TOTAL_30_DAY_FILL_COUNT, coalesceLong(row.getField(TOTAL_30_DAY_FILL_COUNT)));
                    prescribed.setProperty(TOTAL_DAY_SUPPLY, coalesceLong(row.getField(TOTAL_DAY_SUPPLY)));
                    prescribed.setProperty(TOTAL_DRUG_COST, coalesceDouble(row.getField(TOTAL_DRUG_COST)));

                    prescribed.setProperty(BENE_COUNT_GE65, coalesceLong(row.getField(BENE_COUNT_GE65)));
                    prescribed.setProperty(TOTAL_CLAIM_COUNT_GE65, coalesceLong(row.getField(TOTAL_CLAIM_COUNT_GE65)));
                    prescribed.setProperty(TOTAL_30_DAY_FILL_COUNT_GE65, coalesceLong(row.getField(TOTAL_30_DAY_FILL_COUNT_GE65)));
                    prescribed.setProperty(TOTAL_DAY_SUPPLY_GE65, coalesceLong(row.getField(TOTAL_DAY_SUPPLY_GE65)));
                    prescribed.setProperty(TOTAL_DRUG_COST_GE65, coalesceDouble(row.getField(TOTAL_DRUG_COST_GE65)));

                    // Commit to Neo4j and Start a new Transaction
                    if (count % TRANSACTION_LIMIT == 0) {
                        tx.success();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
            tx.success();
        } catch (Exception exception) {
            log.error("Error found in Importing Data - Relationships on row: " +  count);
            log.error(Arrays.stream(exception.getStackTrace())
                    .map(Objects::toString)
                    .collect(Collectors.joining("\n")));
        } finally {
            tx.close();
        }

    }

    private Long coalesceLong(String text) {
        if (StringUtils.isNumeric(text)) {
            return Long.valueOf(text);
        }
        return 0L;
    }

    private Double coalesceDouble(String text) {
        if (StringUtils.isNumeric(text)) {
            return Double.valueOf(text);
        }
        return 0.0;
    }


    private void createRelationship(HashMap<String, Relationship> relationships, Node from, Node to, RelationshipType relationshipType) {
        if (from != null && to != null) {
            String relKey = from.getId() + "-" + to.getId() + "-" + relationshipType.name();
            Relationship rel;
            if (!relationships.containsKey(relKey)) {
                rel = from.createRelationshipTo(to, relationshipType);
            } else {
                rel = relationships.get(relKey);
            }
            // Add Relationship to Relationship Cache
            relationships.put(relKey, rel);
        }
    }

    private void importNodes(HashMap<String, Node> nodeMap, Label label) {
        try(Transaction tx = db.beginTx()) {
            for (Map.Entry<String, Node> entry : nodeMap.entrySet()) {
                if (entry.getValue() == null) {
                    Node node = db.findNode(label, NAME, entry.getKey());
                    if (node == null) {
                        node = db.createNode(label);
                        node.setProperty(NAME, entry.getKey());
                    }
                    nodeMap.put(entry.getKey(), node);
                }
            }
            tx.success();
        }
    }

}