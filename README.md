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
    

Data:
-----

Download and unzip data (warning, large files ahead):

[Medicare Provider Charge Data](https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Part-D-Prescriber.html)


*  [2017](http://download.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Downloads/PartD_Prescriber_PUF_NPI_DRUG_17.zip)
*  [2016](http://download.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Downloads/PartD_Prescriber_PUF_NPI_DRUG_16.zip)
*  [2015](http://download.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Downloads/PartD_Prescriber_PUF_NPI_DRUG_15.zip)
*  [2014](http://download.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Downloads/PartD_Prescriber_PUF_NPI_DRUG_14.zip)
*  [2013](http://download.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Downloads/PartD_Prescriber_PUF_NPI_DRUG_13.zip)

[Exclusion List (fraudsters)](https://oig.hhs.gov/exclusions/exclusions_list.asp)    

* [Latest](https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv)

[FDA Drug Data](https://www.fda.gov/drugs/drug-approvals-and-databases/drugsfda-data-files)

* [Latest](https://www.fda.gov/media/89850/download)

[CMS Physician Shared Patient Patterns Data](https://www.nber.org/data/physician-shared-patient-patterns-data.html)

* [2015 30 Day](https://www.nber.org/physician-shared-patient-patterns/2015/pspp2015_30.zip)
* [2014 30 Day](https://www.nber.org/physician-shared-patient-patterns/2014/pspp2014_30.zip)
* [2013 30 Day](https://www.nber.org/physician-shared-patient-patterns/2013/pspp2013_30.zip)
* [2012 30 Day](https://www.nber.org/physician-shared-patient-patterns/2012/pspp2012_30.zip)
* [2011 30 Day](https://www.nber.org/physician-shared-patient-patterns/2011/pspp2011_30.zip)
* [2010 30 Day](https://www.nber.org/physician-shared-patient-patterns/2010/pspp2010_30.zip)
* [2009 30 Day](https://www.nber.org/physician-shared-patient-patterns/2009/pspp2009_30.zip)

Sequence:
--------


1. Create the Schema:

        CALL com.maxdemarzi.schema.generate();
    
2. Import the PartD files in reverse order (10-15 minutes each):

        CALL com.maxdemarzi.import("/Users/maxdemarzi/Documents/Projects/medicare/neo4j-enterprise-3.5.7/import/PartD_Prescriber_PUF_NPI_Drug_17.txt")
        CALL com.maxdemarzi.import("/Users/maxdemarzi/Documents/Projects/medicare/neo4j-enterprise-3.5.7/import/PartD_Prescriber_PUF_NPI_Drug_16.txt")
        CALL com.maxdemarzi.import("/Users/maxdemarzi/Documents/Projects/medicare/neo4j-enterprise-3.5.7/import/PartD_Prescriber_PUF_NPI_Drug_15.txt")
        CALL com.maxdemarzi.import("/Users/maxdemarzi/Documents/Projects/medicare/neo4j-enterprise-3.5.7/import/PartD_Prescriber_PUF_NPI_Drug_14.txt")
        CALL com.maxdemarzi.import("/Users/maxdemarzi/Documents/Projects/medicare/neo4j-enterprise-3.5.7/import/PartD_Prescriber_PUF_NPI_Drug_13.txt")

3. Import the Exclusions (10 seconds):

        USING PERIODIC COMMIT 
        LOAD CSV WITH HEADERS FROM "file:///exclusions-06-2019.csv" AS line WITH line
        MATCH (p:Provider {npi: line.NPI})
        SET p:Fraudster, p.excluded_date = date(line.EXCLDATE)
        
        