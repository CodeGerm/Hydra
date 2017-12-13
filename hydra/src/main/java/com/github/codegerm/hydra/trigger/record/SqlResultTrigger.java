package com.github.codegerm.hydra.trigger.record;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.trigger.PollableTrigger;
import com.google.common.base.Preconditions;

public class SqlResultTrigger extends PollableTrigger {

	public static final String KEY_TRIGGER_STATUS_FILE = "trigger.statusFile";
	public static final String KEY_TRIGGER_PARAMS = "trigger.parameters";

	private static final Logger logger = LoggerFactory.getLogger(SqlResultTrigger.class);

	private String statusFile;
	private String sql;

	private RecordMonitor recordMonitor;

	@Override
	public void configure(Context context) {
		super.configure(context);

		this.statusFile = context.getString(KEY_TRIGGER_STATUS_FILE);
		this.sql = generateSQL(context.getString(KEY_TRIGGER_PARAMS));

		Preconditions.checkNotNull(statusFile, "Status file is not defined");
		Preconditions.checkNotNull(sql, "SQL is not defined");

		this.recordMonitor = new SqlResultMonitor(context, sql, null);
		logger.info("Sql result trigger configured: sql=" + sql);
	}

	@Override
	public void start() {
		RecordStatus status = new RecordStatus();
		status.load(statusFile);
		recordMonitor.setInitialStatus(status);
		boolean result = recordMonitor.establishSession();
		if (!result) {
			recordMonitor = null;
		}
		super.start();
	}

	@Override
	public void stop() {
		if (recordMonitor != null) {
			recordMonitor.closeSession();
		}
		super.stop();
	}

	@Override
	protected void process() {
		if (recordMonitor != null) {
			RecordStatus status = recordMonitor.checkRecord();
			status.save(statusFile);
			if (status.isTriggered()) {
				triggerActions();
			}
		}
	}

	protected String generateSQL(String parameters) {
		return parameters;
	}

}
