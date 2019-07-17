# Medicare
Import Medicare Data into Neo4j

Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/medicare-1.0-SNAPSHOT.jar`,
that can be copied to the `plugin` directory of your Neo4j instance.

    cp target/medicare-1.0-SNAPSHOT.jar neo4j-enterprise-3.5.6/plugins/.
    

Restart your Neo4j Server. Your new Stored Procedures are available:


    CALL com.maxdemarzi.schema.generate();

    CALL com.maxdemarzi.import(file)
    CALL com.maxdemarzi.import("/Users/maxdemarzi/Documents/Projects/medicare/neo4j-enterprise-3.5.7/import/PartD_Prescriber_PUF_NPI_Drug_17.txt")
    
    