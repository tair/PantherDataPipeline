package org.tair.util;

import com.amazonaws.services.dynamodbv2.xspec.S;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.tair.module.paint.flatfile.GoBasic;
import org.tair.process.panther.PantherLocalWrapper;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;


public class Util {
	static PantherLocalWrapper pantherLocal = new PantherLocalWrapper();

	public static int[] getTaxonFilters() throws Exception {
		List<Integer> taxonIds = new ArrayList<>();
		String taxonIdColumnName = "taxonID";

        // Open the CSV file from the resources folder
        try (CSVReader reader = new CSVReader(new InputStreamReader(
			Util.class.getResourceAsStream("/organism_to_display.csv")))) {
			String[] header = reader.readNext();
            if (header == null) {
                throw new Exception("CSV file is empty or header is missing.");
            }

			// Find the index of the 'taxonID' column
            int taxonIdIndex = -1;
            for (int i = 0; i < header.length; i++) {
                if (header[i].equalsIgnoreCase(taxonIdColumnName)) {
                    taxonIdIndex = i;
                    break;
                }
            }

            if (taxonIdIndex == -1) {
                throw new Exception("Taxon ID column not found in CSV file.");
            }
            // Skip the header
            String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
                // Extract the taxonID using the identified index
                int taxonID = Integer.parseInt(nextLine[taxonIdIndex]);
                taxonIds.add(taxonID);
            }
        } 

        // Convert the list to an array of ints
        return taxonIds.stream().mapToInt(i -> i).toArray();
	}

	public static List<String> getPlantOrganisms() throws Exception {
        List<String> plantOrganisms = new ArrayList<>();
        String organismColumnName = "Organism";
        String isPlantColumnName = "Is plant?";

        // Open the CSV file from the resources folder
        try (CSVReader reader = new CSVReader(new InputStreamReader(
			Util.class.getResourceAsStream("/organism_to_display.csv"), "UTF-8"))) {

			// if(reader == null || reader.readNext() == null || reader.readNext().length == 0) {
			// 	throw new Exception("CSV file is empty or header is missing.");
			// }
            String[] header = reader.readNext();
			// Check if the header contains a BOM and remove it if necessary
			if (header != null && header.length > 0) {
				header[0] = header[0].replace("\uFEFF", ""); // Remove BOM if it exists
			}

            // Find the indexes of the 'Organism' and 'Is plant?' columns
            int organismIndex = -1;
            int isPlantIndex = -1;
            for (int i = 0; i < header.length; i++) {
                if (header[i].equalsIgnoreCase(organismColumnName)) {
                    organismIndex = i;
                }
                if (header[i].equalsIgnoreCase(isPlantColumnName)) {
                    isPlantIndex = i;
                }
            }

            if (organismIndex == -1) {
                throw new Exception("Organism column not found in CSV file.");
            }

            if (isPlantIndex == -1) {
                throw new Exception("Is plant? column not found in CSV file.");
            }

            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                // Filter based on the 'Is plant?' column value
                if (nextLine[isPlantIndex].equalsIgnoreCase("y")) {
                    String organismName = nextLine[organismIndex];
                    plantOrganisms.add(organismName);
                }
            }
        }

        return plantOrganisms;
    }

	public static String readFromFile(String path) throws Exception {

		StringBuilder contentBuilder = new StringBuilder();
		try (Stream<String> stream = Files.lines(Paths.get(path), StandardCharsets.UTF_8)) {
			stream.forEach(s -> contentBuilder.append(s).append("\n"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return contentBuilder.toString();
	}

    //Save Json String as File
    public static void saveJsonStringAsFile(String jsonString, String filepath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        File jsonFile = new File(filepath);
        jsonFile.setExecutable(true);
        jsonFile.setReadable(true);
        jsonFile.setWritable(true);
        jsonFile.createNewFile();
        mapper.writeValue(jsonFile, jsonString);
    }

    //Save Java Object as Json File
	public static void saveJavaObjectAsFile(Object jsonObj, String filepath) throws IOException, Exception {
		ObjectMapper mapper = new ObjectMapper();
		File jsonFile = new File(filepath);
		jsonFile.setExecutable(true);
		jsonFile.setReadable(true);
		jsonFile.setWritable(true);
		jsonFile.createNewFile();
		mapper.writeValue(jsonFile, jsonObj);
	}

	public static String loadJsonStringFromFile(String filepath) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		InputStream input = new FileInputStream(filepath);
		return mapper.readValue(input, String.class);
	}

	public static GoBasic loadJsonFromFile(String filepath) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
//		InputStream is = GoBasic.class.getResourceAsStream(filepath);
		GoBasic obj = mapper.readValue(new File(filepath), GoBasic.class);
		return obj;
	}

	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	public static JSONObject getJsonObjectFromUrl(String url) throws IOException, JSONException {
		InputStream is = new URL(url).openStream();
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
//			System.out.println(json.toString());
			return json;
		} catch(Exception e) {
			return null;
		} finally {
			is.close();
		}
	}

	public static String readJsonFromUrl(String url) throws IOException, JSONException {
		InputStream is = new URL(url).openStream();
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
//			System.out.println(json.toString());
			return json.toString();
		} catch(Exception e) {
            return "";
        } finally {
			is.close();
		}
	}

	public static <T> String readContentFromWebUrlToJson(Class<T> cls, String url) throws Exception {
		BufferedReader in;
		try {
			in = new BufferedReader(new InputStreamReader(new URL(url).openConnection().getInputStream()));
		}
		catch(Exception e) {
			return "";
		}
		StringBuffer buff = new StringBuffer();
		String inputLine;
		while ((inputLine = in.readLine()) != null)
			buff.append(inputLine).append("\n");
		in.close();

		// convert json XML to JSON string
		return XML.toJSONObject(buff.toString()).toString();
	}

	public static <T> String readContentFromWebUrlToJsonString(String url) throws Exception {

		BufferedReader in = new BufferedReader(new InputStreamReader(new URL(url).openConnection().getInputStream()));
		StringBuffer buff = new StringBuffer();
		String inputLine;
		while ((inputLine = in.readLine()) != null)
			buff.append(inputLine).append("\n");
		in.close();

		// convert json XML to JSON string
		return XML.toJSONObject(buff.toString()).toString();
	}
	
	public static <T> String readContentFromWebJsonToJson(String url) throws Exception {

		BufferedReader in = new BufferedReader(new InputStreamReader(new URL(url).openConnection().getInputStream()));
		StringBuffer buff = new StringBuffer();
		String inputLine;
		while ((inputLine = in.readLine()) != null)
			buff.append(inputLine).append("\n");
		in.close();
		
		return buff.toString();
	}

	public static String readFamilyNameFromUrl(String url) throws Exception {

		URL flUrl = new URL(url);
		flUrl.openConnection();
		BufferedReader in = new BufferedReader(new InputStreamReader(new URL(url).openConnection().getInputStream()));
		StringBuffer buff = new StringBuffer();
		String inputLine;
		while ((inputLine = in.readLine()) != null)
			buff.append(inputLine).append("\n");
		in.close();
		return XML.toJSONObject(buff.toString()).toString();
	}

	public static List<String> saxReader(String url) throws IOException, SAXException {
		XMLReader myReader = XMLReaderFactory.createXMLReader();
		MSAHandler handler = new MSAHandler();
		myReader.setContentHandler(handler);
		myReader.parse(new InputSource(new URL(url).openStream()));
		return handler.getSequenceInfo();
	}
}
