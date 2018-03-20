package com.github.codegerm.hydra.source;


public class SqlSourceUtil {
	
	public static final String SNAPSHOT_ID="snapshot_id";
	
	public static enum MODE {
		TASK, SCHEDULE;
	}
	
	//flume config keys
	public static final String WORKER_THREAD_NUM_KEY="snapshot_worker_number";
	public static final String STATUS_BASE_DIR_KEY = "status.file.basepath";
	public static final String STATUS_DIRECTORY_KEY = "status.file.path";
	public static final String POLL_INTERVAL_KEY = "poll.interval";
	public static final String TABLE_KEY = "table";
	public static final String TIMEOUT_KEY = "snapshot_task_timeout";
	public static final String MODEL_INSTANCE_KEY = "model.instance";
	public static final String MODEL_SCHEMA_KEY = "model.schema";
	public static final String MODE_KEY = "mode";
	public static final String TRIGGER_TYPE_KEY = "trigger.type";
	public static final String PAGESIZE_KEY = "snapshot_chunk_size";
	public static final String PAGED_MODE = "paged.mode";
	public static final String TABLE_NAME_REPLACE_ENV = "SNAPSHOT_TABLE_REPLACE";
	public static final String SCHEMA_NAME_REPLACE_ENV = "SNAPSHOT_SCHEMA_REPLACE";
	public static final String TRIGGER_TABLE_NAME_REPLACE_ENV = "SNAPSHOT_TRIGGER_TABLE_REPLACE";
	
	//default values
	public static final int DEFAULT_THREAD_NUM = 8;
	public static final long DEFAULT_SCHEDULE_POLL_INTERVAL = 100000;
	public static final long DEFAULT_TASK_POLL_INTERVAL = 1000;
	public static final long DEFAULT_TIMEOUT = 36000000;
	public static final String DEFAULT_MODE = MODE.TASK.name();
	
}
