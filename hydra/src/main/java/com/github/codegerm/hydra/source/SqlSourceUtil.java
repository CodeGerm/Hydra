package com.github.codegerm.hydra.source;


public class SqlSourceUtil {
	
	public static final String SNAPSHOT_ID="snapshot_id";
	
	public static enum MODE {
		TASK, SCHEDULE;
	}
	
	//flume config keys
	public static final String WORKER_THREAD_NUM_KEY="worker.thread.num";
	public static final String STATUS_BASE_DIR_KEY = "status.file.basepath";
	public static final String STATUS_DIRECTORY_KEY = "status.file.path";
	public static final String POLL_INTERVAL_KEY = "poll.interval";
	public static final String TABLE_KEY = "table";
	public static final String TIMEOUT_KEY = "timeout";
	public static final String MODELMAP_NAME_KEY = "modelmap.name";
	public static final String MODEL_SCHEMA_KEY = "model.schema";
	public static final String MODE_KEY = "mode";
	public static final String TRIGGER_TYPE_KEY = "trigger.type";

	
	//default values
	public static final int DEFAULT_THREAD_NUM = 4;
	public static final long DEFAULT_SCHEDULE_POLL_INTERVAL = 100000;
	public static final long DEFAULT_TASK_POLL_INTERVAL = 1000;
	public static final long DEFAULT_TIMEOUT = 100000;
	public static final String DEFAULT_MODE = MODE.TASK.name();
	
}
