package org.tair.module;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class GOAnnotation {
	private String geneProductId;
	private String goId;
	private String goName;
	private String evidenceCode;
	private String reference;
	private List<ConnectedXref> withFrom;
	
	@JsonProperty("geneProductId")
	private void buildGeneProductId(String geneProductId) {
		if (geneProductId.startsWith("UniProtKB:")) {
			this.geneProductId = geneProductId.split(":")[1];
		} else {
			this.geneProductId = geneProductId;
			try {
			    BufferedWriter writer = new BufferedWriter(new FileWriter("src/main/resources/irregularGeneProductId.txt", true));
			    writer.write(geneProductId+"\n");
			    writer.close();
			}catch (IOException ex) {
	    		ex.printStackTrace();
	    	}
		}
	}
	
    @JsonProperty("withFrom")
    private void buildWithFrom(List<WithFromItem> withFrom) {
    	if (withFrom != null) {    		    	
			List<ConnectedXref> withFromList = new ArrayList<ConnectedXref>();
			for (WithFromItem withFromItem: withFrom) {
				List<ConnectedXref> connectedXrefs =  withFromItem.getConnectedXrefs();
				for (ConnectedXref connectedXref:connectedXrefs) {
					withFromList.add(connectedXref);
				}
			}
	        this.withFrom = withFromList;
    	}else {
    		this.withFrom = null;
    	}
    }
    
    @JsonProperty("evidenceCode")
    private void buildEvidence(String evidenceCode) {
    	Properties prop = new Properties();
    	InputStream input = null;

    	try {

    		input = new FileInputStream("src/main/resources/evidence.properties");

    		// load a properties file
    		prop.load(input);
    		
    		String key = evidenceCode.split(":")[1];
    		this.evidenceCode = prop.getProperty(key);
    		
    	} catch (IOException ex) {
    		ex.printStackTrace();
    	} finally {
    		if (input != null) {
    			try {
    				input.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}
    }
}