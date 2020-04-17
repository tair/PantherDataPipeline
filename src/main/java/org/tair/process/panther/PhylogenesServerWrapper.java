package org.tair.process.panther;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.tair.module.PantherData;
import org.tair.process.PantherBookXmlToJson;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PhylogenesServerWrapper {
    //S3 Keys
    String accessKey = "AKIAT2DXR6T2BY4CBKN2";
    String secretKey = "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";
    String PG_TREE_BUCKET_NAME = "phg-panther-data";
    String PG_MSA_BUCKET_NAME = "phg-msa-data";

    private String URL_SOLR = "http://localhost:8983/solr/panther";
    //		String URL_SOLR = "http://54.68.67.235:8983/solr/panther";

    SolrClient mysolr = null;
    AmazonS3 s3_server = null;
    int committedCount = 0;

    PantherLocalWrapper pantherLocal = new PantherLocalWrapper();

    public PhylogenesServerWrapper() {
        mysolr = new HttpSolrClient.Builder(URL_SOLR).build();
        committedCount = 0;

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        s3_server = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_WEST_2)
                .build();
    }

	public void saveAndCommitToSolr(List<PantherData> pantherList) throws Exception {
        mysolr.addBeans(pantherList);
        mysolr.commit();
		committedCount += pantherList.size();
		System.out.println("Total file commited to solr until now "+committedCount);
	}

	//Update a single field in solr
	public void atomicUpdateSolr(String id, List<String> value) throws Exception {
        //Example code to update just one field
        SolrInputDocument sdoc = new SolrInputDocument();
        sdoc.addField("id", id);
        Map<String, List<String>> partialUpdate = new HashMap<>();
        partialUpdate.put("set", value);
        sdoc.addField("species_list", partialUpdate);
        mysolr.add(sdoc);
        mysolr.commit();
	}

    //Set "uniprot_ids_count" field for panther solr
	public void setUniprotIdsCount() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(8900);
		sq.setFields("id, uniprot_ids");
		sq.setSort("id", SolrQuery.ORDER.asc);

		QueryResponse treeIdResponse = mysolr.query(sq);
		System.out.println(treeIdResponse.getResults().size());

		int totalDocsFound = treeIdResponse.getResults().size();
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			System.out.println("processing: " + i + " "+ treeId); //debugging visualization
			if(treeIdResponse.getResults().get(i).getFieldValues("uniprot_ids") != null) {
				int uniprotIdsCount = treeIdResponse.getResults().get(i).getFieldValues("uniprot_ids").size();
				System.out.println(uniprotIdsCount);
				SolrInputDocument sdoc = new SolrInputDocument();
				Map<String, String> partialUpdate = new HashMap<>();
				partialUpdate.put("set", Integer.toString(uniprotIdsCount));
				sdoc.addField("id", treeId);
				sdoc.addField("uniprot_ids_count", partialUpdate);
                mysolr.add(sdoc);
                mysolr.commit();
			} else {
				System.out.println("No uniprot ids found");
			}
		}
	}

	//Set "go_annotations_count" field for panther solr
	public void setGoAnnotationsCount() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id, go_annotations");
		sq.setSort("id", SolrQuery.ORDER.asc);

		QueryResponse treeIdResponse = mysolr.query(sq);
		System.out.println(treeIdResponse.getResults().size());

		int totalDocsFound = treeIdResponse.getResults().size();
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			System.out.println("processing: " + i + " "+ treeId); //debugging visualization
			if(treeIdResponse.getResults().get(i).getFieldValues("go_annotations") != null) {
				int uniprotIdsCount = treeIdResponse.getResults().get(i).getFieldValues("go_annotations").size();
				System.out.println(uniprotIdsCount);
				SolrInputDocument sdoc = new SolrInputDocument();
				Map<String, String> partialUpdate = new HashMap<>();
				partialUpdate.put("set", Integer.toString(uniprotIdsCount));
				sdoc.addField("id", treeId);
				sdoc.addField("go_annotations_count", partialUpdate);
                mysolr.add(sdoc);
                mysolr.commit();
			} else {
				System.out.println("null");
			}
		}
	}

	//Analyze panther trees and find out all trees with Hori_Transfer node in it. Write the ids to a csv
	public void analyzePantherTrees() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = mysolr.query(sq);

		int totalDocsFound = treeIdResponse.getResults().size();
		System.out.println("totalDocsFound "+ totalDocsFound);
        pantherLocal.initLogWriter(2);
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			PantherData origPantherData = pantherLocal.readPantherTreeById(treeId);
			//Is Horizontal Transfer
			boolean isHorizTransfer = new PantherBookXmlToJson().isHoriz_Transfer(origPantherData);
			if(isHorizTransfer) {
				pantherLocal.logHTId(treeId);
			}
		}
        pantherLocal.initLogWriter(2);
	}

	public void uploadJsonToPGTreeBucket(String filename, String jsonStr) {
        try {
            uploadJsonToS3(PG_TREE_BUCKET_NAME, filename, jsonStr);
        } catch(Exception e) {
            System.out.println("Failed to save to S3 " + e);
        }
    }

    public void uploadJsonToPGMsaBucket(String filename, String jsonStr) {
        try {
            uploadJsonToS3(PG_MSA_BUCKET_NAME, filename, jsonStr);
        } catch(Exception e) {
            System.out.println("Failed to save to S3 " + e);
        }
    }

	/////////////////////////////////////////////////// S3 Calls
    public void createBucket(String bucket_name) throws Exception{
        if(s3_server.doesBucketExistV2(bucket_name)) {
            System.out.println(bucket_name + " already exists");
        }
        s3_server.createBucket(bucket_name);
    }

    public void listAllBuckets() {
        List<Bucket> buckets = s3_server.listBuckets();
        for(Bucket b : buckets) {
            System.out.println(b.getName());
        }
    }

    public void uploadObjectToBucket(String bucketName, String filename, File file) {
        s3_server.putObject(bucketName, filename, file);
    }

    public void uploadObjectToBucket(String bucketName, String filename, String filepath) {
        s3_server.putObject(bucketName, filename, new File(filepath));
    }

    public void uploadJsonToS3(String bucketName, String key, String content) {
        s3_server.putObject(bucketName, key, content);
    }
}
