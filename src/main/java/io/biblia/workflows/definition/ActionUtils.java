package io.biblia.workflows.definition;

import java.util.SortedMap;
import java.util.TreeMap;

import io.biblia.workflows.Configuration;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import io.biblia.workflows.ConfigurationKeys;

public class ActionUtils implements ConfigurationKeys {

	private static final String ROOT_FOLDER = "workflows";
	private static final String DIVIDER = "/";
	public static final String ENCRYPT_DIVIDER = "~";
	private static int MAX_FOLDER_SIZE;
	private static MessageDigest digester;
	
	static
	{
		//1. MAX_FOLDER_SIZE
		final int DEFAULT_MAX_FOLDER_SIZE = 255;
		String sizeS = Configuration.getValue(MAX_FOLDER_SIZE_KEY);
		try {
			MAX_FOLDER_SIZE = Integer.parseInt(sizeS);
		}
		catch(NumberFormatException ex) {
			MAX_FOLDER_SIZE = DEFAULT_MAX_FOLDER_SIZE;
		}
		
		//2. Encryptor
		try{
			digester = MessageDigest.getInstance("SHA-1");
		}
		catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * To be used by command line actions.
	 * @param shortName
	 * @param extraInputs
	 * @param actionConf
	 * @return
	 */
	public static String createActionUniqueNameNaturalOrder(String shortName, LinkedHashMap<String, String> extraInputs, 
			LinkedHashMap<String, String> actionConf) {
		if (extraInputs.size() == 0 && actionConf.size() == 0
				&& shortName.length() <= MAX_FOLDER_SIZE) {
			return shortName;
		}
		else {
			StringBuilder concatenation = new StringBuilder();
			concatenation.append(shortName);
			for (Entry<String, String> e : extraInputs.entrySet()) {
				concatenation.append(e.getKey()).append("*").append(e.getValue());
				concatenation.append(ENCRYPT_DIVIDER);
			}
			for (Entry<String, String> e : actionConf.entrySet()) {
				concatenation.append(e.getKey()).append(ENCRYPT_DIVIDER).append(e.getValue());
				concatenation.append(ENCRYPT_DIVIDER);
			}
			
			return encrypt(concatenation.toString());
		}
	}
	
	/**
	 * To be used by actions that are not command line actions.
	 * @param shortName
	 * @param extraInputs
	 * @param actionConf
	 * @return
	 */
	public static String createActionUniqueNameAlphabeticalOrder(String shortName, LinkedHashMap<String, String> extraInputs, 
			LinkedHashMap<String, String> actionConf) {
		if (extraInputs.size() == 0 && actionConf.size() == 0
				&& shortName.length() <= MAX_FOLDER_SIZE) {
			return shortName;
		}
		else {
			StringBuilder concatenation = new StringBuilder();
			concatenation.append(shortName);
			SortedMap<String, String> sortedExtraInputs = new TreeMap<>(extraInputs);
			for (Entry<String, String> e : sortedExtraInputs.entrySet()) {
				concatenation.append(e.getKey()).append(e.getValue());
			}
			SortedMap<String, String> sortedActionConf = new TreeMap<>(actionConf);
			for (Entry<String, String> e : sortedActionConf.entrySet()) {
				concatenation.append(e.getKey()).append(e.getValue());
			}
			
			return encrypt(concatenation.toString());
		}
	}

	/**
	 * Given an action name and its parents, it creates a long name for the action.
	 * For example, given a parent ['/filter/extraction'] and a shortName 'computation'
	 * It will produce the following name: '/filter/extraction/computation'
	 * 
	 * The long name of the action will also be the name of the output folder, so the
	 * system needs to know what is the size limit of a folder name for the
	 * file system. It also needs to know what characters are allowed or not
	 * in the name of a folder.
	 * 
	 * If 
	 * @param shortName
	 * @param parentsLongNames
	 * @param extraInputs
	 * @param actionConf
	 * @return the long name of the action.
	 */
	public static List<String> createActionLongNameAlphabeticalOrder(String uniqueName, 
			List<List<String>> parentsLongNames
			) {
		
		List<String> toReturn = new ArrayList<String>();
		
		if (parentsLongNames.size() == 0) {
			toReturn.add(ROOT_FOLDER);
		}
		else {
			toReturn = generateLongNameFromParentNamesAlphabeticalOrder(parentsLongNames);
		}
		toReturn.add(uniqueName);
		return toReturn;
	}
	
	public static List<String> createActionLongNameNaturalOrder(String uniqueName,
			List<List<String>> parentsLongNames) {
		List<String> toReturn = new ArrayList<String>();
		
		if (parentsLongNames.size() == 0) {
			toReturn.add(ROOT_FOLDER);
		}
		else {
			toReturn = generateLongNameFromParentNamesNaturalOrder(parentsLongNames);
		}
		toReturn.add(uniqueName);
		return toReturn;
	}
	
	/**
	 * If parents.size == 0 returns empty list
	 * else if parents.size == 1 returns the longName of the parent
	 * as the only String in the list.
	 * else if parents.size > 1 returns uses the long names of all the parents
	 * to produce the new name
	 * @param parents
	 * @return
	 */
	private static List<String> generateLongNameFromParentNamesAlphabeticalOrder(List<List<String>> parents) {
		
		List<String> toReturn = new ArrayList<String>();
		
		if (parents.size() == 1) {
			toReturn = parents.get(0);
		}
		else if (parents.size() > 1) {
			//Generate name from all parent names
			StringBuilder concatenation = new StringBuilder();
			List<String> parentNames = new ArrayList<String>();
			
			for (List<String> longNames : parents) {
				parentNames.add(longNames.get(longNames.size() - 1));
			}
			Collections.sort(parentNames);
			for (String name : parentNames) {
				concatenation.append(name);
				concatenation.append(ENCRYPT_DIVIDER);
			}
			
			toReturn.add(encrypt(concatenation.toString()));
		}
		return toReturn;
	}
	
	private static List<String> generateLongNameFromParentNamesNaturalOrder(List<List<String>> parents) {
		List<String> toReturn = new ArrayList<String>();
		
		if (parents.size() == 1) {
			toReturn = parents.get(0);
		}
		else if (parents.size() > 1) {
			//Generate name from all parent names
			StringBuilder concatenation = new StringBuilder();
			List<String> parentNames = new ArrayList<String>();
			
			for (List<String> longNames : parents) {
				parentNames.add(longNames.get(longNames.size() - 1));
			}
			for (String name : parentNames) {
				concatenation.append(name);
				concatenation.append(ENCRYPT_DIVIDER);
			}
			
			toReturn.add(encrypt(concatenation.toString()));
		}
		return toReturn;
	}
	
	/**
	 * Converts a list of strings that represents folder hierarchy into a
	 * valid folder name
	 * @param longName
	 * @return
	 */
	public static String generateOutputPathFromLongName(List<String> longName) {
		
		StringBuilder concatenation = new StringBuilder();
		for (String name : longName) {
			concatenation.append(ENCRYPT_DIVIDER);
			concatenation.append(name);
		}
		
		
		return DIVIDER + ROOT_FOLDER + DIVIDER + encrypt(concatenation.toString());
	}
	
	/**
	 * Returns an encrypted String using the SHA-1 algorithm.
	 * @param string
	 * @return
	 */
	private static String encrypt(String string) {
		if (null == string || string.length() == 0) {
			throw new IllegalArgumentException("String to encrypt cannot be null"
					+ " or of zero length");
		}
		digester.update(string.getBytes());
		byte[] hash = digester.digest();
		StringBuffer hexString = new StringBuffer();
		for (int i = 0; i < hash.length; ++i) {
			if ((0xff & hash[i]) < 0x10) {
				hexString.append("0" + Integer.toHexString((0xFF & hash[i])));
			}
			else {
				hexString.append(Integer.toHexString(0xFF & hash[i]));
			}
		}
		return hexString.toString();
	}
}
