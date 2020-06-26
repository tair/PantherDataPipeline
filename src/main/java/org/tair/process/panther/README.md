# ETL Pipeline for Panther Data
PantherETLPipeline.java runs the main etl to transform panther data to phylogenes server.
PantherServerWrapper.java is class to handle all Panther Server side code.
PantherLocalWrapper.java is class to handle all local machine code. (We download all panther data to local machine first, and then do sole indexing
PhylogenesServerWrapper.java is class to handle all Phylogenes solr server and S3 bucket connections.

Please check application.properties, and make sure all the server urls are correct. Make sure the fields from Panther server match
the fields you want to retreive in module.panther package.

In main() function run the following:
1. storePantherFilesLocally(): If you don't already have files stored locally, or you want to update local files with the panther server.
Local file structure after downloading is done:
-- familyList: Has list of panther family ids and names (batch of 1000)
    -- familyList_1.json
    |
    |
    -- familyList_1001.json
-- pruned_panther_files: has all panther json files after pruning using taxon ids (specified in the PantherSerevrWrapper
    -- Deleted: Has panther files which are deleted since no plant genomes were found in the tree
-- familyNoPlantsList.csv - Has logs of all ids which don't have plant genomes and have been deleted
-- msa_jsons: has all msa json files from the panther server
-- panther_jsons: has all panther json files along with solr indexing, similar to what is stored on solr server
-- familyEmptyWhileIndexingList.csv - All panther ids which errored out, or were empty while indexing or downloading from server.

2. uploadToServer(): Uploads the files to solr server and s3 buckets which are already setup.
--
