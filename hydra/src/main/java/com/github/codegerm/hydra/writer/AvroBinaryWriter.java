package com.github.codegerm.hydra.writer;

import java.util.List;

import org.apache.flume.channel.ChannelProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroBinaryWriter extends AbstractAvroWriter {

	public static final String WRITER_TYPE = "avro";

	private static final Logger LOG = LoggerFactory.getLogger(AvroBinaryWriter.class);

	public AvroBinaryWriter(ChannelProcessor processor, String snapshotId, String entitySchema) {
		super(processor, snapshotId, entitySchema);
	}

	@Override
	protected String getWriterType() {
		return WRITER_TYPE;
	}

	@Override
	protected byte[] serializeEvent(List<Object> record, String schema) {
		try {
			return AvroRecordUtil.serializeToBinary(record, entitySchema);
		} catch (Exception e) {
			LOG.warn("Event serialize error: ", e);
			return null;
		}
	}

}
