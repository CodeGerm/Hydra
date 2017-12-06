package com.github.codegerm.hydra.trigger.record;

import org.apache.flume.Context;

import com.github.codegerm.hydra.trigger.PollableTrigger;
import com.google.common.base.Preconditions;

public class NewRecordTrigger extends PollableTrigger {

	public static final String KEY_TRIGGER_STATUS_FILE = "trigger.statusFile";
	public static final String KEY_TABLE_NAME = "trigger.tableName";
	public static final String KEY_PRIMARY_KEY_NAME = "trigger.primaryKeyName";

	private String statusFile;
	private String tableName;
	private String primaryKeyName;

	private RecordMonitor recordMonitor;

	@Override
	public void configure(Context context) {
		super.configure(context);

		this.statusFile = context.getString(KEY_TRIGGER_STATUS_FILE);
		this.tableName = context.getString(KEY_TABLE_NAME);
		this.primaryKeyName = context.getString(KEY_PRIMARY_KEY_NAME);

		Preconditions.checkNotNull(statusFile, "Status file is not defined");
		Preconditions.checkNotNull(tableName, "Table name is not defined");

		this.recordMonitor = new NewRecordMonitor(context, tableName, primaryKeyName, null);
	}

	@Override
	public void start() {
		RecordStatus status = new RecordStatus();
		status.load(statusFile);
		recordMonitor.setInitialStatus(status);
		recordMonitor.establishSession();
		super.start();
	}

	@Override
	public void stop() {
		recordMonitor.closeSession();
		super.stop();
	}

	@Override
	protected void process() {
		RecordStatus status = recordMonitor.checkRecord();
		status.save(statusFile);
		if (status.isTriggered()) {
			triggerActions();
		}
	}

}
