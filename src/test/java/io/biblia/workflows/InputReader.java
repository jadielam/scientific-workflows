package io.biblia.workflows;

import java.io.IOException;

import org.apache.commons.io.IOUtils;

public class InputReader {

	public static String getFile(String fileName) throws IOException {
		
		String result = "";
		
		ClassLoader classLoader = InputReader.class.getClassLoader();
		try {
			result = IOUtils.toString(classLoader.getResourceAsStream(fileName));
		}
		catch(IOException e) {
			throw e;
		}
		
		return result;
	}
}
