package org.tair.process.uniprotdb;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.tair.util.Util;

import com.fasterxml.jackson.databind.ObjectWriter;

public class TestGOAnnotationUrlToJson {
	ObjectWriter ow = new ObjectMapper().writer();
	
	@Test
	public void TestReadFromUrl() throws Exception {
		System.out.println("======TestReadFromUrl======");
		String uniprot_id= "Q23624,K4BQA1,F6HKV6,O22230,I1M1P7,I1K431,K7L661";
		String url = "https://www.ebi.ac.uk/QuickGO/services/annotation/search?includeFields=goName&aspect=molecular_function&geneProductId="+uniprot_id+"&evidenceCode=ECO%3A0000314%2C%20ECO%3A0000315%2C%20ECO%3A0000316%2C%20ECO%3A0000270%2C%20ECO%3A0000269%2C%20ECO%3A0000353&qualifier=enables&geneProductType=protein";
		String jsonString = Util.readContentFromWebJsonToJson(url);
		System.out.println(jsonString);
	}
	
	@Test
	public void TestReadGOAnnotationUrlToObject() throws JsonProcessingException, Exception {
		System.out.println("======TestReadGOAnnotationUrlToObject======");
		String sampleGOAnnotationDataStr = ow.writeValueAsString(new GOAnnotationUrlToJson().readGOAnnotationUrlToObject("o75431"));
		System.out.println(sampleGOAnnotationDataStr);
	}

	@Test
	public void TestReadGOAnnotationUrlToObjectList() throws JsonProcessingException, Exception {
		System.out.println("======TestReadGOAnnotationUrlToObjectList======");
		String sampleGOAnnotationDataStr = ow.writeValueAsString(new GOAnnotationUrlToJson().readGOAnnotationUrlToObjectList("Q23624,C0LGG6,Q9BSG1,Q7JR71,o75431"));
		System.out.println(sampleGOAnnotationDataStr);
	}

}
