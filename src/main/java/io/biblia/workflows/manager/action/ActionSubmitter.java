package io.biblia.workflows.manager.action;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.apache.oozie.client.OozieClientException;

import com.google.common.base.Preconditions;

import io.biblia.workflows.oozie.OozieClientUtil;
import io.biblia.workflows.definition.parser.WorkflowParseException;

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
	final Logger logger = Logger.getLogger(ActionSubmitter.class.getName());

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
			logger.log(Level.FINE, "Updated the action state of action {0} to SUBMITTED", action.get_id());
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
			persistance.addActionSubmissionId(action, submissionId);
			logger.log(Level.INFO, "Oozie client submitted action {0}", action.get_id());
		} catch (OozieClientException | IOException | WorkflowParseException | OutdatedActionException ex) {
			ex.printStackTrace();
			logger.log(Level.FINE, "Oozie client was not able to submit action {0}", action.get_id());

			// 1.2.1.1 If there is an error submitting action, update
			// database with state: READY.
			try {
				action = this.persistance.updateActionState(action, ActionState.READY);
				logger.log(Level.FINE, "Action {0} state was changed back to READY", action.get_id());
			} catch (OutdatedActionException ex1) {
				logger.log(Level.FINE, "Action {0} state could not be changed back to READY because of OutdatedActionException", action.get_id());
				return;
			} catch (Exception e) {
				logger.log(Level.FINE, "Action {0} state could not be changed back to READY because of unknown exception", action.get_id());
				return;
			}

			return;
		}
		try {
			action = this.persistance.addActionSubmissionId(action, submissionId);
			logger.log(Level.FINE, "Added the submission id {0} to action {1}", new Object[] {action.get_id(), submissionId});
		} catch (OutdatedActionException ex) {
			logger.log(Level.FINE, "Could not updated action {0} with submission id {1}", new Object[] {action.get_id(), submissionId});
		}
		catch (Exception e) {
			logger.log(Level.FINE, "Could not updated action {0} with submission id {1}", new Object[] {action.get_id(), submissionId});
			logger.log(Level.WARNING, "The following exception was thrown: " + e.toString(), e);
			return;
		}
	}

}
