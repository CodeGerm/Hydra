package com.github.codegerm.hydra.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.event.EventBuilder;
import com.github.codegerm.hydra.event.SqlEventBuilder;

public class AvroWriter implements RecordWriter{

	private static final Logger LOG = LoggerFactory.getLogger(AvroWriter.class);
	private ChannelProcessor processor;
	private String entitySchema;
	private List<Event> events = new ArrayList<>();
	public static final String WRITER_TYPE = "avro";
	private Map<String, String> header;
	
	public AvroWriter(ChannelProcessor processor, String entitySchema) {
		this.processor = processor;
		this.entitySchema = entitySchema;
		header = new HashMap<String, String>();
		header.put(EventBuilder.WRITER_TYPE_KEY, WRITER_TYPE);
		String entityName = AvroRecordUtil.getSchemaName(entitySchema);
		if(entityName!=null)
			header.put(EventBuilder.ENTITY_NAME_KEY, entityName);
	}

	public void writeAll(List<List<Object>> records) {
		for (List<Object> record : records) {
			try {
				byte[] body = AvroRecordUtil.serialize(record, entitySchema);
				Event event = SqlEventBuilder.build(body, header);
				events.add(event);
			} catch (Exception e) {
				LOG.warn("Event serialize error: ", e);
			}
		}
		
	}
	
	@Override
	public void flush(){
		if (events != null && !events.isEmpty()) {
			LOG.info("Flushing: " + events.size() + " event(s): ");
			processor.processEventBatch(events);
		}
		events.clear();
	}
	
	@Override
	public void close() {
		flush();
	}



}
