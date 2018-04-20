package com.github.codegerm.hydra.event;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusEventBuilder implements EventBuilder {
	private static final Logger logger = LoggerFactory.getLogger(StatusEventBuilder.class);
	public static final String SNAPSHOT_BEGIN_TYPE = "snapshot.begin";
	public static final String SNAPSHOT_END_TYPE = "snapshot.end";
	public static final String SNAPSHOT_ERROR_TYPE = "snapshot.error";
	public static final String TABLE_BEGIN_TYPE = "table.begin";
	public static final String TABLE_END_TYPE = "table.end";

	public static final String STATUS_TYPE_KEY = "statusType";
	public static final String SNAPSHOT_ID_KEY = "snapshotId";
	public static final String SNAPSHOT_TIMEOUT_KEY = "snapshotCreationTimeout";
	
	
	public static final String EVENT_TYPE = "Status.Event";
	
	private static final int msgLenLimit = 250;

	protected static Event buildStatusEvent(String statusType, String snapshotId, String modelId, String entity) {
		Event event = new SimpleEvent();
		String timestamp = Long.toString(System.currentTimeMillis());
		Map<String, String>header = new HashMap<String, String>();
		header.put(SNAPSHOT_ID_KEY, snapshotId);
		header.put(STATUS_TYPE_KEY, statusType);
		header.put(TIMESTAMP_KEY, timestamp);
		header.put(EVENT_TYPE_KEY, EVENT_TYPE);
		header.put(EventBuilder.MODEL_ID_KEY, modelId);
		if(entity!=null){
			if(entity.length()>msgLenLimit) {
				logger.warn("msg: " + entity +"is longer than limit: " + msgLenLimit +", truncating");
				entity = entity.substring(0,  msgLenLimit);
			}
			header.put(ENTITY_NAME_KEY, entity);
		}
		event.setHeaders(header);
		return event;
	}
	
	
	
	public static Event buildSnapshotBeginEvent(String snapshotId, String modelId, Long snapshotCreationTimeout){
		Event event = buildStatusEvent(SNAPSHOT_BEGIN_TYPE, snapshotId, modelId, null);
		event.getHeaders().put(SNAPSHOT_TIMEOUT_KEY, snapshotCreationTimeout.toString());
		return event;
	}
	
	public static Event buildSnapshotEndEvent(String snapshotId, String modelId){
		return buildStatusEvent(SNAPSHOT_END_TYPE, snapshotId, modelId, null);
	}
	
	public static Event buildTableBeginEvent(String snapshotId, String modelId, String tableName){
		return buildStatusEvent(TABLE_BEGIN_TYPE, snapshotId, modelId, tableName);
	}
	
	public static Event buildTableEndEvent(String snapshotId, String modelId, String tableName){
		return buildStatusEvent(TABLE_END_TYPE, snapshotId, modelId, tableName);
	}
	
	public static Event buildSnapshotErrorEvent(String snapshotId, String modelId, String msg){
		return buildStatusEvent(SNAPSHOT_ERROR_TYPE, snapshotId, modelId, msg);
	}
	

}
