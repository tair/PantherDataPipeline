package org.tair.process;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Region;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.tair.module.MsaData;
import org.tair.module.PantherData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class PantherS3Wrapper {
    String accessKey = "AKIAT2DXR6T2BY4CBKN2";
    String secretKey = "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";
    AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
    AmazonS3 s3client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.US_WEST_2)
            .build();

    String BUCKET_NAME = "test-swapp-bucket";

    public void createBucket() throws Exception{

        if(s3client.doesBucketExistV2(BUCKET_NAME)) {
            System.out.println("already exists");
        }
        s3client.createBucket(BUCKET_NAME);
    }

    public void listAllBuckets() {
        List<Bucket> buckets = s3client.listBuckets();
        for(Bucket b : buckets) {
            System.out.println(b.getName());
        }
    }

    public void uploadObjectToBucket(String bucketName, String filename, File file) {
        s3client.putObject(bucketName, filename, file);
    }

    public void uploadObjectToBucket(String bucketName, String filename, String filepath) {
        s3client.putObject(bucketName, filename, new File(filepath));
    }

    public void uploadJsonToS3(String bucketName, String key, String content) {
        s3client.putObject(bucketName, key, content);
    }

}
