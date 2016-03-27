package io.biblia.workflows.statistics;

import java.util.HashMap;
import java.util.Map;

import com.mongodb.MongoClient;

import io.biblia.workflows.EnvironmentVariables;

public class WorkflowsDAOBuilder implements EnvironmentVariables {
	
	public static final String MONGODB_TYPE = "mongodb";
	public static final String DEFAULT_TYPE = MONGODB_TYPE;
	
	private static abstract class DAOBuilder {
		
		private WorkflowsDAO instance;
		
		public WorkflowsDAO getDAOInstance() {
			if (null == instance) {
				instance = createDAO();
			}
			return instance;
		}
		
		protected abstract WorkflowsDAO createDAO();
		
	}
	
	private static final Map<String, DAOBuilder> buildersMap;
	static {
		buildersMap = new HashMap<String, DAOBuilder>();
		buildersMap.put(MONGODB_TYPE, new DAOBuilder() {
			
			@Override
			protected WorkflowsDAO createDAO() {
				MongoClient client = MongoDBClientFactory.getInstance();
				return WorkflowsMongoDAO.getInstance(client);
			}
			
		});
		
	}
	
	public static WorkflowsDAO getInstance(String type) {
		if (null == type) {
			type = DEFAULT_TYPE;
		}
		if (buildersMap.containsKey(type)) {
			return buildersMap.get(type).getDAOInstance();
		}
		else {
			return buildersMap.get(DEFAULT_TYPE).getDAOInstance();
		}
	}

}
