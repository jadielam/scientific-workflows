package io.biblia.workflows.manager.action;

import java.io.IOException;

import org.apache.oozie.client.OozieClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.biblia.workflows.oozie.OozieClientUtil;

/**
 * Submits an action to be run. The medium where it will
 * be ran depends on the type of the action.  We are using
 * Oozie as an intermediary right now on the submission
 * of the action.
 * @author dearj019
 *
 */
public class ActionSubmitter implements Runnable {

	private final PersistedAction action;
	private final ActionPersistance persistance;
	final Logger logger = LoggerFactory.getLogger(ActionSubmitter.class);

	public ActionSubmitter(PersistedAction action, ActionPersistance persistance) {
		Preconditions.checkNotNull(action);
		Preconditions.checkNotNull(persistance);
		this.action = action;
		this.persistance = persistance;
	}

	@Override
	public void run() {

		try {
			this.submitAction(this.action);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Submits an action to hadoop to be processed.
	 * 
	 * @param action
	 */
	private void submitAction(PersistedAction action) {

		// 1.2, Update database with submitted
		try {
			action = this.persistance.updateActionState(action, ActionState.SUBMITTED);
			logger.debug("Updated the action state of action {} to SUBMITTED", action.get_id());
		} catch (OutdatedActionException ex) {
			return;
		} catch (Exception e) {
			return;
		} 

		// 1.2.1 If database accepts update by comparing versions
		// Submit to Oozie.
		String submissionId = null;
		try {
			submissionId = OozieClientUtil.submitAndStartOozieJob(action.getAction());
			logger.debug("Oozie client submitted action {}", action.get_id());
		} catch (OozieClientException | IOException ex) {
			ex.printStackTrace();
			logger.debug("Oozie client was not able to submit action {}", action.get_id());

			// 1.2.1.1 If there is an error submitting action, update
			// database with state: READY.
			try {
				action = this.persistance.updateActionState(action, ActionState.READY);
				logger.debug("Action {} state was changed back to READY", action.get_id());
			} catch (OutdatedActionException ex1) {
				logger.debug("Action {} state could not be changed back to READY because of OutdatedActionException", action.get_id());
				return;
			} catch (Exception e) {
				logger.debug("Action {} state could not be changed back to READY because of unknown exception", action.get_id());
				return;
			}

			return;
		}
		try {
			action = this.persistance.addActionSubmissionId(action, submissionId);
			logger.debug("Added the submission id {} to action {}", action.get_id(), submissionId);
		} catch (OutdatedActionException ex) {
			logger.error("Could not updated action {} with submission id {}", action.get_id(), submissionId);
			// This exception is not supposed to be thrown in here. Log the
			// error
			// as a bug to be fixed later on.
		}
		catch (Exception e) {
			return;
		}
	}

}
