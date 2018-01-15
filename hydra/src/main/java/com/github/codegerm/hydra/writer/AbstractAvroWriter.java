package com.github.codegerm.hydra.writer;

import org.apache.flume.channel.ChannelProcessor;

public abstract class AbstractAvroWriter extends AbstractRecordWriter {

	public AbstractAvroWriter(ChannelProcessor processor, String snapshotId, String modelId, String entitySchema) {
		super(processor, snapshotId, modelId, entitySchema);
	}

	@Override
	protected String getEntityNameFromSchema(String schema) {
		return AvroRecordUtil.getSchemaName(entitySchema);
	}

}
