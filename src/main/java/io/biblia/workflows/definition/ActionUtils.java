package io.biblia.workflows.definition;

import java.util.Set;

import io.biblia.workflows.Configuration;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map.Entry;

public class ActionUtils {

	private static final String MAX_FOLDER_SIZE_KEY = "workflows.definition.maxFolderSize";
	private static final String ROOT_FOLDER = "workflows";
	private static final String DIVIDER = "/";
	private static int MAX_FOLDER_SIZE;
	
	{
		final int DEFAULT_MAX_FOLDER_SIZE = 255;
		String sizeS = Configuration.getValue(MAX_FOLDER_SIZE_KEY);
		try {
			MAX_FOLDER_SIZE = Integer.parseInt(sizeS);
		}
		catch(NumberFormatException ex) {
			MAX_FOLDER_SIZE = DEFAULT_MAX_FOLDER_SIZE;
		}
	}
	
	/**
	 * 
	 * @param shortName
	 * @param extraInputs
	 * @param actionConf
	 * @return
	 */
	public static String createActionUniqueName(String shortName, LinkedHashMap<String, String> extraInputs, 
			LinkedHashMap<String, String> actionConf) {
		if (extraInputs.size() == 0 && actionConf.size() == 0
				&& shortName.length() <= MAX_FOLDER_SIZE) {
			return shortName;
		}
		else {
			StringBuilder concatenation = new StringBuilder();
			concatenation.append(shortName);
			for (Entry<String, String> e : extraInputs.entrySet()) {
				concatenation.append(e.getKey()).append(e.getValue());
			}
			for (Entry<String, String> e : actionConf.entrySet()) {
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
	 * @param parents
	 * @param extraInputs
	 * @param actionConf
	 * @return the long name of the action.
	 */
	public static List<String> createActionLongName(String uniqueName, 
			Set<Action> parents
			) {
		
		List<String> toReturn = new ArrayList<String>();
		
		if (parents.size() == 0) {
			toReturn.add(ROOT_FOLDER);
		}
		else {
			toReturn = generateLongNameFromParentNames(parents);
		}
		toReturn.add(uniqueName);
		return toReturn;
	}
	
	/**
	 * If parents.size == 0 returns empty list
	 * else if parents.size == 1 returns the longName of the parent
	 * as the only String in the lsit.
	 * else if parents.size > 1 returns uses the long names of all the parents
	 * to produce the new name
	 * @param parents
	 * @return
	 */
	private static List<String> generateLongNameFromParentNames(Set<Action> parents) {
		
		List<String> toReturn = new ArrayList<String>();
		
		if (parents.size() == 1) {
			for (Action parent : parents) {
				toReturn = parent.getLongName();
			}
		}
		else if (parents.size() > 1) {
			//Generate name from all parent names
			StringBuilder concatenation = new StringBuilder();
			List<String> parentNames = new ArrayList<String>();
			
			for (Action parent : parents) {
				parentNames.add(parent.getUniqueName());
			}
			Collections.sort(parentNames);
			for (String name : parentNames) {
				concatenation.append(name);
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
		StringBuilder toReturn = new StringBuilder("");
		for (String name : longName) {
			toReturn.append(DIVIDER);
			toReturn.append(name);
		}
		return toReturn.toString();
	}
	
	/**
	 * Returns an encrypted String that of size less than or equal to the MAX_FOLDER_SIZE.
	 * It also takes care of not using certain String characters on its encryption, mainly
	 * characters that will not make valid names, such as "/", " ", etc.
	 * @param string
	 * @return
	 */
	private static String encrypt(String string) {
		//TODO
		return string;
	}
}
