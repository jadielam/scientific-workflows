package io.biblia.workflows;

import java.io.FileInputStream;
import java.util.Properties;
import java.io.InputStream;

/**
 * Class that reads configuration from file system
 * and makes it available to the other classes of the system.
 * @author dearj019
 *
 */
public class Configuration {

	static private final Properties configuration;
	
	static
	{
		configuration = new Properties();
		String confPath = System.getenv("SW_CONFIGURATION_FILE");
		
		if (null != confPath) {
			try{
				InputStream is = new FileInputStream(confPath);
				configuration.load(is);
			}
			catch(Exception e) {
				e.printStackTrace();
			}	
		}		
	}
	
	/**
	 * Returns the value for the property. Returns null if not found.
	 * @param prop
	 * @return
	 */
	public static String getValue(String prop) {
		return configuration.getProperty(prop);
	}
	
	/**
	 * Returns the value for the property. Returns the passed defaultValue
	 * if not found.
	 * @param prop
	 * @param defaultValue
	 * @return
	 */
	public static String getValue(String prop, String defaultValue) {
		return configuration.getProperty(prop, defaultValue);
	}
}
