package com.maxdemarzi.schema;

import org.neo4j.graphdb.RelationshipType;

public enum RelationshipTypes implements RelationshipType {
    HAS_SPECIALTY,
    IN_LOCATION,
    HAS_GENERIC,
    PRESCRIBED
}