package com.github.codegerm.hydra.writer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.channel.ChannelProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class AvroJsonWriter extends AbstractAvroWriter {

	public static final String WRITER_TYPE = "json";

	private static final Logger LOG = LoggerFactory.getLogger(AvroJsonWriter.class);
	private static final Gson GSON = new GsonBuilder().create();

	public AvroJsonWriter(ChannelProcessor processor, String snapshotId, String entitySchema) {
		super(processor, snapshotId, entitySchema);
	}

	@Override
	protected String getWriterType() {
		return WRITER_TYPE;
	}

	@Override
	protected byte[] serializeEvents(List<List<Object>> records, String schema) {
		List<String> result = new ArrayList<>();
		for (List<Object> record : records) {
			byte[] eventJson = serializeEvent(record, schema);
			if (eventJson != null) {
				result.add(new String(eventJson, StandardCharsets.UTF_8));
			}
		}
		return GSON.toJson(result).getBytes();
	}

	private byte[] serializeEvent(List<Object> record, String schema) {
		try {
			return AvroRecordUtil.serializeToJson(record, schema);
		} catch (Exception e) {
			LOG.warn("Event serialize error: ", e);
			return null;
		}
	}

}
