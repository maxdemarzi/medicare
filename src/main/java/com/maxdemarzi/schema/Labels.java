package com.maxdemarzi.schema;

import org.neo4j.graphdb.Label;

public enum Labels implements Label {
    Specialty,
    Location,
    Drug,
    Generic,
    Provider
}