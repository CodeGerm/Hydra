package com.github.codegerm.hydra.handler;

import java.util.concurrent.Callable;

import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import com.github.codegerm.hydra.event.StatusEventBuilder;

public abstract class AbstractHandler implements Callable<Boolean> {

	protected Context context;
	protected ChannelProcessor processor;
	protected String modelId;
	protected String table;
	protected String snapshotId;
	protected String entitySchema;
	
	public AbstractHandler(String snapshotId, Context context, ChannelProcessor processor, String modelId, String table, String entitySchema) {
		initialize(snapshotId,  context,processor, modelId, table, entitySchema);
		configure();
	}
	
	public AbstractHandler(){
	}
	
	public void initialize(String snapshotId, Context context, ChannelProcessor processor, String modelId, String table, String entitySchema){
		this.context = context;
		this.processor = processor;
		this.modelId = modelId;
		this.table = table;
		this.snapshotId = snapshotId;
		this.entitySchema = entitySchema;
	}
	
	@Override
	public Boolean call() {

		processor.processEvent(StatusEventBuilder.buildTableBeginEvent(snapshotId, modelId, table));
		Boolean success = handle();
		if(success){
			processor.processEvent(StatusEventBuilder.buildTableEndEvent(snapshotId, modelId, table));
			return true;
		} else 
			return false;
	}
	
	public abstract Boolean handle();

	public abstract void configure();


}
