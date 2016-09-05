package io.biblia.workflows.manager;

public interface DatabaseConstants {

	String DATASETS_COLLECTION = "datasets_cl";
	String DATASETS_LOG_COLLECTION = "datasets_log_cl";
	String WORKFLOWS_LOG_COLLECTION = "workflows_log_cl";
	String ACTIONS_COLLECTION = "actions_cl";
	String COUNTERS_COLLECTION = "counters";
	String WORKFLOWS_DATABASE = "workflows_db";
	
	
    /**
     * The number of seconds for an action in PROCESSING state to be
     * considered obsolete.
     */
	int OUTDATED_SECONDS = 400;
}
