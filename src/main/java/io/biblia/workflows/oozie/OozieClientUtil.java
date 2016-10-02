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
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClientException;

import com.google.common.base.Preconditions;

import io.biblia.workflows.ConfigurationKeys;
import io.biblia.workflows.definition.Action;
import io.biblia.workflows.hdfs.HdfsUtil;

public class OozieClientUtil implements ConfigurationKeys {

	private static FileSystem fs = null;
	
	private static final OozieClient client;

	private static final String END_NODE_NAME = "done";

	private static final String WORKFLOW_DEFINITION_FILE_NAME = "workflow.xml";

	static {
		client = new OozieClient(io.biblia.workflows.Configuration.getValue(OOZIE_URL));
		String NAMENODE_URL = io.biblia.workflows.Configuration.getValue(NAMENODE);
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", NAMENODE_URL);
		try{
			fs = FileSystem.get(conf);
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param ManagedAction
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
			conf.setProperty("jobTracker", io.biblia.workflows.Configuration.getValue(JOBTRACKER));
			conf.setProperty("nameNode", io.biblia.workflows.Configuration.getValue(NAMENODE));
			// 6. Return the job id of the action.
			String jobId = client.run(conf);
			return jobId;

		} catch (OozieClientException e) {
			throw e;
		}
	}
	
	/**
	 * Returns a list with two elements, where the first element
	 * is the start date of the jobid, and the second element
	 * is the end date of the jobid.
	 * Returns a list with two null entries
	 * @param jobId
	 * @return
	 */
	public static List<Date> getStartAndEndTime(String jobId) {
		List<Date> toReturn = new ArrayList<Date>();
		toReturn.add(null);
		toReturn.add(null);
		try{
			WorkflowJob info = client.getJobInfo(jobId);
			if (null != info) {
				Date start = info.getStartTime();
				Date end = info.getEndTime();
				toReturn.set(0, start);
				toReturn.set(1, end);
			}
		}
		catch(OozieClientException ex) {
			ex.printStackTrace();
		}
		return toReturn;
	}

	/**
	 * @throws UnsupportedOperationException
	 *             whenever the runtime type of the action parameter does not
	 *             have an implemented translation to an Oozie action type yet.
	 */
	private static OozieAction convertToOozieAction(Action action) throws UnsupportedOperationException {

		if (action instanceof io.biblia.workflows.definition.CommandLineAction) {
			io.biblia.workflows.definition.CommandLineAction javaAction = (io.biblia.workflows.definition.CommandLineAction) action;
			String actionName = javaAction.getOriginalName();
			String okName = END_NODE_NAME;
			String errorName = END_NODE_NAME;
			String mainClassName = javaAction.getMainClassName();
			Map<String, String> inputParameters = javaAction.getInputParameters();
			Map<String, String> additionalInput = javaAction.getExtraInputs();
			String output = javaAction.getOutputPath();
			Map<String, String> configurationParameters = javaAction.getConfiguration();
			List<String> arguments = new ArrayList<>();
			if (null != inputParameters) {
				arguments.addAll(inputParameters.values());
			}
			if (null != additionalInput) {
				arguments.addAll(additionalInput.values());
			}
			arguments.add(output);
			if (null != configurationParameters) {
				arguments.addAll(configurationParameters.values());
			}
			
			String nameNode = javaAction.getNameNode();
			String jobTracker = javaAction.getJobTracker();
			JavaAction oAction = new JavaAction(actionName, okName, errorName, mainClassName, arguments, jobTracker,
					nameNode);
			return oAction;
		}
		else
			throw new UnsupportedOperationException("Could not convert from class type: "
					+ action.getClass().getSimpleName() + " to any known Oozie action type");
	}

	private static void writeWorkflowDefinition(String workflowDefinition, String folderPath) throws IOException {
		HdfsUtil.writeStringToFile(workflowDefinition, folderPath, WORKFLOW_DEFINITION_FILE_NAME);
	}

	public static void killJob(String jobId) throws OozieClientException {
		Preconditions.checkNotNull(jobId);
		client.kill(jobId);
	}

}
