package io.biblia.workflows.manager.oozie;

import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;

import com.google.common.base.Preconditions;

import io.biblia.workflows.EnvironmentVariables;
import io.biblia.workflows.definition.Action;

public class OozieClientUtil implements EnvironmentVariables {

	private static final OozieClient client;
  
   private static final String END_NODE_NAME = "done";
	
	static {
		client = new OozieClient(SW_OOZIE_URL);
	}
	
  /**
   * @param Action action to submit to Oozie
   * @return the id of the workflow submitted to Oozie.
   */
	public static String submitAndStartOozieJob(Action action) throws OozieClientException {
		Preconditions.checkNotNull(action);
		try {
        
         //1. Convert the action to an Oozie action
        	OozieAction oAction = convertToOozieAction(action); 
        
         //2. Create a workflow with that action as the only action
         OozieWorkflowGenerator xmlGenerator = new OozieWorkflowGenerator();
         xmlGenerator.addName(oAction.getName());
         xmlGenerator.addStartNode(oAction.getName());
         xmlGenerator.addEndNode(END_NODE_NAME);
         xmlGenerator.addAction(oAction);
         
         String xmlWorkflow = xmlGenerator.generateWorkflow();
         
         //3. Save the workflow in the hdfs folder of the action
         String actionFolder = action.getActionFolder();
         //Calling utils.
         
         
         //4. Create a properties file with the properties needed
         //so that Hadoop knows how to find the action data.
         //
         //5. Submit and start the action to oozie.
         //
         //6. Return the job id of the action.

			String jobId = client.run(conf);
			return jobId;
		} catch (OozieClientException e) {
			throw e;
		}
	}
	
   /**
    * @throws UnsupportedOperationException whenever the runtime type of the action parameter
    * does not have an implemented translation to an Oozie action type yet. 
    */
  	private static OozieAction convertToOozieAction(Action action) throws UnsupportedOperationException {
    	
     	if (action instanceof io.biblia.workflows.definition.actions.JavaAction) {
       	io.biblia.workflows.definition.actions.JavaAction javaAction = (io.biblia.workflows.definition.actions.JavaAction) action;
         String actionName = javaAction.getName();
       	String okName = END_NODE_NAME;
        	String errorName = END_NODE_NAME;
        	String mainClassName = javaAction.getMainClassName();
         Map<String, String> inputParameters = javaAction.getInputParameters();
         Map<String, String> outputParameters = javaAction.getOutputParameters();
         Map<String, String> configurationParameters = javaAction.getConfigurationParameters();
         List<String> arguments = new ArrayList<>();
         arguments.addAll(inputParameters.values());
         arguments.addAll(outputParameters.values());
         arguments.addAll(configurationParameters.values());
         JavaAction oAction = new JavaAction(actionName, okName, errorName, mainClassName, arguments);
         return oAction;
      }
      else throw new UnsupportedOperationException("Could not convert from class type: " + 
                                                   action.getClass().getSimpleName() +
                                                  " to any known Oozie action type");
   }
	public static void killJob(String jobId) throws OozieClientException {
		Preconditions.checkNotNull(jobId);
		client.kill(jobId);
	}
	
	
	
	
}
