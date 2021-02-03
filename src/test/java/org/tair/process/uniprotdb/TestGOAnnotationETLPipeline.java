package org.tair.process.uniprotdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.tair.module.GOAnnotation;
import org.tair.process.uniprotdb_iba.GOAnnotationIBAETLPipeline;

public class TestGOAnnotationETLPipeline {
	GOAnnotationIBAETLPipeline GOAnnotationETLPipeline = new GOAnnotationIBAETLPipeline();
	
	@Test
	public void TestReadFromLocal() throws IOException {
		long startTime = System.nanoTime();
		
		String thisLine = null;
		List<String> evidenceCodes = new ArrayList<String>(Arrays.asList("ECO:0000314","ECO:0000315","ECO:0000316","ECO:0000270","ECO:0000269","ECO:0000353"));
		File file = new File("/Users/qian/Downloads/goa_uniprot_gcrp.gpa");
		FileReader fileReader = new FileReader(file);
		BufferedReader br = new BufferedReader(fileReader);
		// skip first 25 line
		for (int i=0;i<25;i++) {
			br.readLine();
		}
//		int loops = 0;
		int count = 0;
		while ((thisLine = br.readLine()) != null) {
//			if ( ++loops >= 10000000) break;
			String[] attributes = thisLine.split("\t");
			if (!attributes[0].equals("UniProtKB")) {
				continue;
			}
			if (!attributes[2].equals("enables")) {
				continue;
			}
			if (!evidenceCodes.contains(attributes[5])) {
				continue;
			}
//			if (!attributes[6].contains("|") || !attributes[6].contains(",")) {
//				continue;
//			}
			for (String attribute : attributes) {
				System.out.print(attribute+ " ");
			}
			System.out.println();
			GOAnnotation goAnnotation = new GOAnnotation();
			goAnnotation.setGeneProductId(attributes[1]);
			goAnnotation.setGoId(attributes[3]);
			goAnnotation.setReference(attributes[4]);
			goAnnotation.setEvidenceCode(attributes[5]);
//			List<String> withFrom = Arrays.asList(attributes[6].split("[|,]"));
			// need to parse every withFrom to fit the structure of ConnectedXref {db:"",id:""}
//			goAnnotation.setWithFrom(withFrom);
			System.out.println(goAnnotation.toString());
			count ++;
		}
		br.close();
		System.out.println("total annotations processed: "+ count);
		
		long endTime = System.nanoTime();		
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in milliseconds : " + timeElapsed / 1000000);
	}
	
//	@Test
//    public void TestSingleCommit() throws IOException, SolrServerException, Exception {
//    	String uniprot_id = "Q23624";
//    	GOAnnotationETLPipeline GOAnnotationETLPipeline = new GOAnnotationETLPipeline();
//    	GOAnnotationETLPipeline.solrClient.addBeans("uniprot_db",GOAnnotationETLPipeline.GOAnnotationUrlToJson.readGOAnnotationUrlToObjectList(uniprot_id));
//    	GOAnnotationETLPipeline.solrClient.commit("uniprot_db");
//    	System.out.println("Stored: "+ uniprot_id);
//    }
    
//629500-630000 larger than 25; 630000-630500 error; 480000-480500 more than 25 pages
//	for (int j = 630000; j<630500; j++) {
//		uniprotIdList.add(counts.get(j).getName());
//	}
	
}
