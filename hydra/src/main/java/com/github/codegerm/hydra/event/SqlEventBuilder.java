package com.github.codegerm.hydra.event;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;

public class SqlEventBuilder {

	public static Event build(byte[] body) {
		Event event = new SimpleEvent();
		Map<String, String> headers;
		headers = new HashMap<String, String>();
		headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
		event.setHeaders(headers);
		event.setBody(body);
		return event;
	}

}
