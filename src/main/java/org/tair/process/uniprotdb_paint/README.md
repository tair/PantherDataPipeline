# ETL Pipeline for GO Annotation PAINT Data

- GO_PAINT_Pipeline.java file is used to index the paint_db collection of the solr database.
- PantherUpdateGOAnnotations.java is used to update go_annotations field in panther collection with the data in paint_db.

## Methods to Load Data

1. Download GO Annotation PAINT TSV Data from Panther URL
   We use method downloadPAINTFilesLocally() in GO_PAINT_Pipeline.java
   In this method, the following process will happen:

- Download and unzip the tsv files from <ftp://ftp.pantherdb.org/downloads/paint/17.0/2022-03-10/Pthr_GO_17.0.tsv.tar.gz>. Replace this URL with the latest paint file
- Download obo file from <http://current.geneontology.org/ontology/go-basic.obo>.

3. Update Solr with uniprot mapping to iba annnotation
   Use method updatePAINTGOFromLocalToSolr() in GO_PAINT_Pipeline.java

** important: if the url of paint file or obo file changes, we need to update them in applications.properties file, otherwise it may not reflect the correct data; if the format of tsv file or obo file has been changed, we need to change the code accordingly. **
