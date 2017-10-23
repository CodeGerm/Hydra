package com.github.codegerm.hydra.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.AbstractSource;

public class JDBCSource extends AbstractSource implements Configurable, PollableSource{

	public void setChannelProcessor(ChannelProcessor channelProcessor) {
		// TODO Auto-generated method stub
		
	}

	public ChannelProcessor getChannelProcessor() {
		// TODO Auto-generated method stub
		return null;
	}

	public void start() {
		// TODO Auto-generated method stub
		
	}

	public void stop() {
		// TODO Auto-generated method stub
		
	}

	public LifecycleState getLifecycleState() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setName(String name) {
		// TODO Auto-generated method stub
		
	}

	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	public Status process() throws EventDeliveryException {
		// TODO Auto-generated method stub
		return null;
	}

	public long getBackOffSleepIncrement() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMaxBackOffSleepInterval() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void configure(Context context) {
		// TODO Auto-generated method stub
		
	}

}
