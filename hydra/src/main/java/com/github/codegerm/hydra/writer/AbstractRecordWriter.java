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
import com.github.codegerm.hydra.event.StatusEventBuilder;

public abstract class AbstractRecordWriter implements RecordWriter {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractAvroWriter.class);
	private static final int DEFAULT_BATCH_SIZE = 10;

	protected ChannelProcessor processor;	
	protected String entitySchema;
	protected Map<String, String> header;
	protected List<Event> events = new ArrayList<>();

	public AbstractRecordWriter(ChannelProcessor processor, String snapshotId, String modelId, String entitySchema) {
		this.processor = processor;		
		this.entitySchema = entitySchema;

		header = new HashMap<String, String>();
		header.put(StatusEventBuilder.SNAPSHOT_ID_KEY, snapshotId);
		header.put(EventBuilder.WRITER_TYPE_KEY, getWriterType());
		header.put(EventBuilder.MODEL_ID_KEY, modelId);
		String entityName = getEntityNameFromSchema(entitySchema);
		if (entityName != null) {
			header.put(EventBuilder.ENTITY_NAME_KEY, entityName);
		}
	}

	@Override
	public void writeAll(List<List<Object>> records) {
		int batchSize = getEventBatchSize();
		for (int i = 0; i < records.size(); i += batchSize) {
			int end = (i + batchSize) > records.size() ? records.size() : (i + batchSize);
			List<List<Object>> subRecords = records.subList(i, end);
			byte[] body = serializeEvents(subRecords, entitySchema);
			if (body != null) {
				Event event = SqlEventBuilder.build(body, header);
				events.add(event);
			}
		}
	}

	@Override
	public void flush() {
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

	protected int getEventBatchSize() {
		return DEFAULT_BATCH_SIZE;
	}

	protected abstract String getWriterType();

	protected abstract String getEntityNameFromSchema(String schema);

	protected abstract byte[] serializeEvents(List<List<Object>> records, String schema);

}
