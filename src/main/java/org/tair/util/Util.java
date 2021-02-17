package org.tair.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.json.simple.parser.JSONParser;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
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
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class Util {
	static PantherLocalWrapper pantherLocal = new PantherLocalWrapper();
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
