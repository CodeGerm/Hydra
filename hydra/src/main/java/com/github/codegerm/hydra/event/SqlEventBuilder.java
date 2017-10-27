package com.github.codegerm.hydra.event;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;

public class SqlEventBuilder implements EventBuilder {
	
	public static final String EVENT_TYPE = "Model.Event";

	public static Event build(byte[] body) {
		Event event = new SimpleEvent();
		Map<String, String> header;
		header = new HashMap<String, String>();
		header.put(TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis()));
		header.put(EVENT_TYPE_KEY, EVENT_TYPE);
		event.setHeaders(header);
		event.setBody(body);
		return event;
	}
	
	public static Event build(byte[] body, Map<String, String> header) {
		Event event = new SimpleEvent();
		header.put(TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis()));
		header.put(EVENT_TYPE_KEY, EVENT_TYPE);
		event.setHeaders(header);
		event.setBody(body);
		return event;
	}

}
