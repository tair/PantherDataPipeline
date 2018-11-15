package org.tair.util;

import org.json.XML;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Util {

	public static String readFromFile(String path) throws Exception {

		StringBuilder contentBuilder = new StringBuilder();
		try (Stream<String> stream = Files.lines(Paths.get(path), StandardCharsets.UTF_8)) {
			stream.forEach(s -> contentBuilder.append(s).append("\n"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return contentBuilder.toString();
	}

	public static <T> String readContentFromWebUrlToJson(Class<T> cls, String url) throws Exception {

		BufferedReader in = new BufferedReader(new InputStreamReader(new URL(url).openConnection().getInputStream()));
		StringBuffer buff = new StringBuffer();
		String inputLine;
		while ((inputLine = in.readLine()) != null)
			buff.append(inputLine).append("\n");
		in.close();

		// convert json XML to JSON string
		return XML.toJSONObject(buff.toString()).toString();
	}
}
