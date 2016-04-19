package io.biblia.workflows.oozie;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;

import com.google.common.base.Preconditions;

import io.biblia.workflows.EnvironmentVariables;
import io.biblia.workflows.definition.Action;

public class OozieClientUtil implements EnvironmentVariables {

	private static final OozieClient client;

	private static final String END_NODE_NAME = "done";

	private static final String WORKFLOW_DEFINITION_FILE_NAME = "workflow.xml";

	static {
		client = new OozieClient(SW_OOZIE_URL);
	}

	/**
	 * @param Action
	 *            action to submit to Oozie
	 * @return the id of the workflow submitted to Oozie.
	 * @throws OozieClientException
	 *             whenever there is problem submitting the action to Oozie.
	 * @throws IOException
	 *             whenever there is problem writing the workflow to hdfs.
	 */
	public static String submitAndStartOozieJob(Action action) throws OozieClientException, IOException {
		Preconditions.checkNotNull(action);
		try {

			// 1. Convert the action to an Oozie action
			OozieAction oAction = convertToOozieAction(action);

			// 2. Create a workflow with that action as the only action
			OozieWorkflowGenerator xmlGenerator = new OozieWorkflowGenerator();
			xmlGenerator.addName(oAction.getName());
			xmlGenerator.addStartNode(oAction.getName());
			xmlGenerator.addEndNode(END_NODE_NAME);
			xmlGenerator.addAction(oAction);

			String xmlWorkflow = xmlGenerator.generateWorkflow();

			// 3. Save the workflow in the hdfs folder of the action
			String actionFolder = action.getActionFolder();
			try {
				writeWorkflowDefinition(xmlWorkflow, actionFolder);
			} catch (IOException ex) {
				throw ex;
			}

			// 4. Create a properties file with the properties needed
			// so that Hadoop knows how to find the action data.

			Properties conf = client.createConfiguration();
			conf.setProperty(OozieClient.APP_PATH, action.getActionFolder());

			// TODO: I don't see the reason why I need to submit namenode
			// and job tracker here.

			conf.setProperty("jobTracker", OozieWorkflowGenerator.DEFAULT_JOB_TRACKER);
			conf.setProperty("nameNode", OozieWorkflowGenerator.DEFAULT_NAME_NODE);

			// TODO: Add a way to specify a callback endpoint in the properties
			// or in the workflow definition xml.
			// 6. Return the job id of the action.
			String jobId = client.run(conf);
			return jobId;

		} catch (OozieClientException e) {
			throw e;
		}
	}

	/**
	 * @throws UnsupportedOperationException
	 *             whenever the runtime type of the action parameter does not
	 *             have an implemented translation to an Oozie action type yet.
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
			String nameNode = javaAction.getName();
			String jobTracker = javaAction.getJobTracker();
			JavaAction oAction = new JavaAction(actionName, okName, errorName, mainClassName, arguments, jobTracker,
					nameNode);
			return oAction;
		}
		else if (action instanceof io.biblia.workflows.definition.actions.FSDeleteAction) {
			io.biblia.workflows.definition.actions.FSDeleteAction fsDelete = (io.biblia.workflows.definition.actions.FSDeleteAction) action;
			String actionName = fsDelete.getName();
			String okName = END_NODE_NAME;
			String errorName = END_NODE_NAME;
			String pathToDelete = fsDelete.getPathToDelete();
			FSDeleteAction deleteAction = new FSDeleteAction(actionName, okName, errorName, pathToDelete);
			return deleteAction;
		}

		else
			throw new UnsupportedOperationException("Could not convert from class type: "
					+ action.getClass().getSimpleName() + " to any known Oozie action type");
	}

	private static void writeWorkflowDefinition(String workflowDefinition, String folderPath) throws IOException {

		// 1. Convert workflowDefintion to input stream
		InputStream in = new ByteArrayInputStream(workflowDefinition.getBytes(StandardCharsets.UTF_8));

		// 2. Create outputStream with correct path
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(folderPath), conf);
		String workflowPath = folderPath + "/" + WORKFLOW_DEFINITION_FILE_NAME;
		OutputStream out = fs.create(new Path(workflowPath));

		// 3. Write from one to another
		IOUtils.copyBytes(in, out, 4096, true);
	}

	public static void killJob(String jobId) throws OozieClientException {
		Preconditions.checkNotNull(jobId);
		client.kill(jobId);
	}

}
