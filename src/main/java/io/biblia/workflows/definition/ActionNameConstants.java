package io.biblia.workflows.definition;

/**
 * Represents different types of actions supported
 * by the system.
 * @author jadiel
 *
 */
public interface ActionNameConstants {

	/**
	 * Command line actions
	 */
	String COMMAND_LINE_ACTION = "command-line";
	
	/**
	 * Actions that use the older map reduce api.
	 */
	String MAP_REDUUCE_1_ACTION = "map-reduce-1";
	
	/**
	 * Actions that use the newest map reduce api.
	 */
	String MAP_REDUCE_2_ACTION = "map-reduce-2";
	
	String FS_DELETE_ACTION = "fs-delete";


}
