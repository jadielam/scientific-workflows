package io.biblia.workflows.manager.oozie;

import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;

import com.google.common.base.Preconditions;

import io.biblia.workflows.EnvironmentVariables;

public class OozieClientUtil implements EnvironmentVariables {

	private static final OozieClient client;
	
	static {
		client = new OozieClient(SW_OOZIE_URL);
	}
	
	public static String submitAndStartOozieJob(Properties conf) throws OozieClientException {
		Preconditions.checkNotNull(conf);
		try {
			//Submits and starts a workflow job.
			String jobId = client.run(conf);
			return jobId;
		} catch (OozieClientException e) {
			throw e;
		}
	}
	
	public static void killJob(String jobId) throws OozieClientException {
		Preconditions.checkNotNull(jobId);
		client.kill(jobId);
	}
	
	
	
	
}
