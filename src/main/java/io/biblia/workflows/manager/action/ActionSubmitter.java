package io.biblia.workflows.manager.action;

import java.io.IOException;

import org.apache.oozie.client.OozieClientException;

import com.google.common.base.Preconditions;

import io.biblia.workflows.oozie.OozieClientUtil;

public class ActionSubmitter implements Runnable {

	private final PersistedAction action;
	private final ActionPersistance persistance;

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
		} catch (OozieClientException | IOException ex) {
			ex.printStackTrace();

			// 1.2.1.1 If there is an error submitting action, update
			// database with state: READY.
			try {
				action = this.persistance.updateActionState(action, ActionState.READY);
			} catch (OutdatedActionException ex1) {
				return;
			} catch (Exception e) {
				return;
			}

			return;
		}
		try {
			action = this.persistance.addActionSubmissionId(action, submissionId);
		} catch (OutdatedActionException ex) {
			// This exception is not supposed to be thrown in here. Log the
			// error
			// as a bug to be fixed later on.
		}
		catch (Exception e) {
			return;
		}
	}

}
