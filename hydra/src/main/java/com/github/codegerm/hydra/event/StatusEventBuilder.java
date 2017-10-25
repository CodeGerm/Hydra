package com.github.codegerm.hydra.event;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;

public class StatusEventBuilder {

	public static final String SNAPSHOT_BEGIN_TYPE = "snapshot.begin";
	public static final String SNAPSHOT_END_TYPE = "snapshot.end";
	public static final String TABLE_BEGIN_TYPE = "table.begin";
	public static final String TABLE_END_TYPE = "table.end";

	public static final String STATUS_TYPE_KEY = "status.type";
	public static final String SNAPSHOT_ID_KEY = "snapshot.id";
	public static final String TIMESTAMP_KEY = "timestamp";
	public static final String TABLE_KEY = "table.name";

	protected static Event buildStatusEvent(String statusType, String snapshotId, String table) {
		Event event = new SimpleEvent();
		String timestamp = Long.toString(System.currentTimeMillis());
		Map<String, String>header = new HashMap<String, String>();
		header.put(SNAPSHOT_ID_KEY, snapshotId);
		header.put(STATUS_TYPE_KEY, statusType);
		header.put(TIMESTAMP_KEY, timestamp);
		if(table!=null)
			header.put(TABLE_KEY, table);
		event.setHeaders(header);
		return event;
	}
	
	
	
	public static Event buildSnapshotBeginEvent(String snapshotId){
		return buildStatusEvent(SNAPSHOT_BEGIN_TYPE, snapshotId, null);
	}
	
	public static Event buildSnapshotEndEvent(String snapshotId){
		return buildStatusEvent(SNAPSHOT_END_TYPE, snapshotId, null);
	}
	
	public static Event buildTableBeginEvent(String snapshotId, String tableName){
		return buildStatusEvent(TABLE_BEGIN_TYPE, snapshotId, tableName);
	}
	
	public static Event buildTableEndEvent(String snapshotId, String tableName){
		return buildStatusEvent(TABLE_END_TYPE, snapshotId, tableName);
	}

}
