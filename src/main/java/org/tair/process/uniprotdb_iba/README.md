# ETL Pipeline for GO Annotation IBA GAF Data

- GO_IBA_Pipeline.java file is used to index the uniprot_db collection of the solr database.
- GOAnnotationUrlToJson.java provides helper methods to read data from url.
- PantherUpdateGOAnnotations.java is used to update go_annotations field in panther collection with the data in uniprot_db.

## Methods to Load Data

<!-- 1. Load GO Annotation Data from API Url
We use method storeGOAnnotationFromApiToUniprotDb() in GOAnnotationETLPipeline.java to load GO Annotation data from QuickGO API url.
In this method, the following process will happen:
- Delete all current data of uniprot_db collection.
- Read xml file from QuickGO API(<https://www.ebi.ac.uk/QuickGO/>) and convert it to JSON format.
- Parse the json string and store data in GOAnnotation object.
- Commit GOAnnotation object to uniprot_db collection of solr database. -->

2. Download GO Annotation IBA GAF Data from Panther URL
   We use method downloadIBAFilesLocally() in GO_IBA_Pipeline.java
   In this method, the following process will happen:

- Download and unzip all gaf files from <ftp://ftp.pantherdb.org/downloads/paint/presubmission>.
- Download obo file from <http://current.geneontology.org/ontology/go-basic.obo> and convert obo file to properties file.

3. Update Solr with uniprot mapping to iba annnotation
   Use method updateIBAGOFromLocalToSolr() in GO_IBA_Pipeline.java

- Read and parse all gaf files and get the attributes of an annotation per line.
- Commit GOAnnotation object to uniprot_db collection of solr database.

** important: if the url of gaf file or obo file changes, we need to update them in applications.properties file, otherwise it may not reflect the correct data; if the format of gaf file or obo file has been changed, we need to change the code accordingly. **
