package com.github.codegerm.hydra.source;

import java.util.concurrent.Callable;

import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import com.github.codegerm.hydra.event.StatusEventBuilder;

public abstract class AbstractHandler implements Callable<Boolean> {

	protected Context context;
	protected ChannelProcessor processor;
	protected String table;
	protected String snapshotId;
	protected String entitySchema;
	
	public AbstractHandler(String snapshotId, Context context, ChannelProcessor processor, String table, String entitySchema) {
		this.context = context;
		this.processor = processor;
		this.table = table;
		this.snapshotId = snapshotId;
		this.entitySchema = entitySchema;
		configure();
	}
	
	@Override
	public Boolean call() {

		processor.processEvent(StatusEventBuilder.buildTableBeginEvent(snapshotId, table));
		Boolean success = handle();
		if(success){
			processor.processEvent(StatusEventBuilder.buildTableEndEvent(snapshotId, table));
			return true;
		} else 
			return false;
	}
	
	public abstract Boolean handle();

	public abstract void configure();


}
