package com.github.codegerm.hydra.trigger.record;

import java.io.File;

import org.apache.flume.Context;

import com.github.codegerm.hydra.trigger.PollableTrigger;

public class NewRecordTrigger extends PollableTrigger {

	public static final String KEY_TRIGGER_STATUS_FILE = "statusFile";
	public static final String KEY_TABLE_NAME = "tableName";
	public static final String KEY_PRIMARY_KEY_NAME = "primaryKeyName";

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

		this.statusFile = new File("tmp/status.file").getAbsolutePath();
		this.tableName = "actor2";
		this.primaryKeyName = "actor_id";

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
