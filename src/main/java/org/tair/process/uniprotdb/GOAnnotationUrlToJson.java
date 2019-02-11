package org.tair.process.uniprotdb;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.tair.module.GOAnnotation;
import org.tair.module.GOAnnotationData;
import org.tair.util.Util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.json.JSONArray;
import org.json.JSONObject;

public class GOAnnotationUrlToJson {

	public GOAnnotationData readGOAnnotationUrlToObject(String uniprot_id) throws Exception {
		String url = "https://www.ebi.ac.uk/QuickGO/services/annotation/search?includeFields=goName&aspect=molecular_function&geneProductId="+uniprot_id+"&evidenceCode=ECO%3A0000314%2C%20ECO%3A0000315%2C%20ECO%3A0000316%2C%20ECO%3A0000270%2C%20ECO%3A0000269%2C%20ECO%3A0000353&qualifier=enables&geneProductType=protein";
		String jsonString = Util.readContentFromWebJsonToJson(url);
		System.out.println(jsonString);
		JSONObject obj = new JSONObject(jsonString);
		JSONArray results = obj.getJSONArray("results");
		List<GOAnnotation> goAnnotations = new ArrayList<GOAnnotation>();
		for (int i = 0; i < results.length(); i++) {
			goAnnotations.add(new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(results.getJSONObject(i).toString(),GOAnnotation.class));
		}
		GOAnnotationData goAnnotationData = new GOAnnotationData();
		goAnnotationData.setUniprot_id(uniprot_id);
		ObjectWriter ow = new ObjectMapper().writer();
		goAnnotationData.setGo_annotations(ow.writeValueAsString(goAnnotations));
		return goAnnotationData;
	}
	
	public List<GOAnnotationData> readGOAnnotationUrlToObjectList(String uniprot_ids) throws Exception {
		String url = "https://www.ebi.ac.uk/QuickGO/services/annotation/search?limit=100&page=1&includeFields=goName&aspect=molecular_function&geneProductId="+uniprot_ids+"&evidenceCode=ECO%3A0000314%2C%20ECO%3A0000315%2C%20ECO%3A0000316%2C%20ECO%3A0000270%2C%20ECO%3A0000269%2C%20ECO%3A0000353&qualifier=enables&geneProductType=protein";
		String jsonString = Util.readContentFromWebJsonToJson(url);
		System.out.println(jsonString);
		JSONObject obj = new JSONObject(jsonString);
		/* The api allows at most 25 pages to be shown so if the result contains more than 25 pages we split input string 
		 * into halves and will recursively do this until it's less than 25 pages. If the input is a single uniprot id and 
		 * the result is still more than 25 pages exception will be thrown*/
		int pages = Integer.parseInt(obj.getJSONObject("pageInfo").get("total").toString());
		List<String> uniprotList = Arrays.asList(uniprot_ids.split(","));
		if (pages > 25) {
			int uniprotListLen = uniprotList.size();
			if (uniprotListLen == 1) {
				throw new Exception("Single uniprotId having too many results.");
			}
			List<GOAnnotationData> firstHalf = readGOAnnotationUrlToObjectList(String.join(",", uniprotList.subList(0, uniprotListLen/2)));
			List<GOAnnotationData> secondHalf = readGOAnnotationUrlToObjectList(String.join(",", uniprotList.subList(uniprotListLen/2, uniprotListLen)));
			firstHalf.addAll(secondHalf);
			return firstHalf;
		}
		JSONArray results = obj.getJSONArray("results");
		List<GOAnnotationData> goAnnotations = new ArrayList<GOAnnotationData>();
		for (int i = 0; i < results.length(); i++) {
			addAnnotationToListByPage(results, i, uniprotList, goAnnotations);
		}
		/* If there are more than 1 page, iterate and store results on each page */
		for (int page = 2; page<= pages; page++) {
			String moreUrl = "https://www.ebi.ac.uk/QuickGO/services/annotation/search?limit=100&page="+page+"&includeFields=goName&aspect=molecular_function&geneProductId="+uniprot_ids+"&evidenceCode=ECO%3A0000314%2C%20ECO%3A0000315%2C%20ECO%3A0000316%2C%20ECO%3A0000270%2C%20ECO%3A0000269%2C%20ECO%3A0000353&qualifier=enables&geneProductType=protein";
			String moreJsonString = Util.readContentFromWebJsonToJson(moreUrl);
			System.out.println(moreJsonString);
			JSONObject moreObj = new JSONObject(moreJsonString);
			JSONArray moreResults = moreObj.getJSONArray("results");
			for (int i = 0; i < moreResults.length(); i++) {
				addAnnotationToListByPage(results, i, uniprotList, goAnnotations);
			}
		}
		return goAnnotations;
	}
	
	private void addAnnotationToListByPage(JSONArray results, int i, List<String> uniprotList, List<GOAnnotationData> goAnnotations) throws Exception {
		GOAnnotation goAnnotation = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(results.getJSONObject(i).toString(),GOAnnotation.class);
		GOAnnotationData goAnnotationData = new GOAnnotationData();
		String geneProductId = goAnnotation.getGeneProductId().toLowerCase();
		// check if geneProductId equals to one of uniprot id in the list
		if (!uniprotList.contains(geneProductId)) {
			Boolean found = false;
			// if not, check if geneProductId contains one of the uniprot id in the list. Use uniprot id as geneProductId
			for (String uniprotId : uniprotList) {
				if (geneProductId.contains(uniprotId.toLowerCase())){
					geneProductId = uniprotId.toLowerCase();
					found = true;
					break;
				}
			}
			// if not found, write it to file and stop the function.
			if (!found) {
				try {
				    BufferedWriter writer = new BufferedWriter(new FileWriter("src/main/resources/GeneProductIdNotEqualToUniprotId.txt", true));
				    writer.write(uniprotList.toString()+"\n");
				    writer.write(geneProductId+"\n");
				    writer.close();
				}catch (IOException ex) {
		    		ex.printStackTrace();
		    	}
				return;
			}
		}
		goAnnotation.setGeneProductId(geneProductId);
		goAnnotationData.setUniprot_id(geneProductId);
		ObjectWriter ow = new ObjectMapper().writer();
		goAnnotationData.setGo_annotations(ow.writeValueAsString(goAnnotation));
		goAnnotations.add(goAnnotationData);
	}
}
