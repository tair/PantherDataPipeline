package org.tair.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.tair.module.MsaData;
import org.tair.module.SearchResult;
import org.tair.module.SequenceList;
import org.tair.util.Util;

import java.util.List;

public class PantherMsaXmlToJson {

	private MsaData msaData = null;
	private ObjectWriter ow = new ObjectMapper().writer();

	public MsaData readMsaById(String id) throws Exception {

		// read the xml data from web.
		String base_url = "http://pantherdb.org/tempFamilySearch?type=msa_info&book=" + id;
		String url = "http://pantherdb.org/tempFamilySearch?type=msa_info&book=" + id
			 + "&taxonFltr=13333,3702,15368,51351,3055,2711,3659,4155,3847,3635,4232,112509,3880,214687,4097,39947,70448,42345,3218,3694,3760,3988,4555,4081,4558,3641,4565,29760,4577,29655,6239,7955,44689,7227,83333,9606,10090,10116,559292,284812";
		try{
			String jsonString = Util.readContentFromWebUrlToJson(MsaData.class, url);

			// convert json string to MsaData object
			this.msaData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
			MsaData.class);
		}catch (OutOfMemoryError oe){
			List<String> sequenceInfoList = Util.saxReader(url);
			SearchResult searchResult = new SearchResult();
			SequenceList sequenceList = new SequenceList();
			this.msaData = new MsaData();

			sequenceList.setSequence_info(sequenceInfoList);
			searchResult.setSequence_list(sequenceList);
			this.msaData.setSearch(searchResult);
		}

		return this.msaData;

	}

	public static void main(String s[]) throws Exception {

		PantherMsaXmlToJson msa = new PantherMsaXmlToJson();
		//example with multiple sequence_info
//		System.out.println(msa.readMsaById("PTHR10000"));
		//example with single sequence_info
//		System.out.println(msa.readMsaById("PTHR39529"));
		//example with large msa data
//		msa.readMsaById("PTHR24015");
	}

}
