package com.github.codegerm.hydra.source;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlSource extends AbstractSource implements Configurable, PollableSource {

	private static final String TRIGGER_EVENTTYPE = "jdbc_event";
	private ExecutorService executor;
	private static final Logger LOG = LoggerFactory.getLogger(SqlSource.class);
	private List<String> models;
	private static final int DEFAULT_THREAD_NUM = 4;
	private static final int DEFAULT_POLL_INTERVAL = 100000;
	private int pollInterval;
	private Context context;

	public void configure(Context context) {
		LOG.info("Start configuring SqlSource");
		this.context = context;
		models = new ArrayList<String>();
		if (context.containsKey("table")) {
			String content[] = context.getString("table").split(",");
			models.addAll(Arrays.asList(content));
		}
		int	thread = context.getInteger(SqlSourceUtil.WORKER_THREAD_NUM, DEFAULT_THREAD_NUM);
		pollInterval = context.getInteger(SqlSourceUtil.POLL_INTERVAL_KEY, DEFAULT_POLL_INTERVAL);
		executor = Executors.newFixedThreadPool(thread);
		
	}

	public void start() {
		super.start();
	}

	public void stop() {
		LOG.info("Stopping SqlSource");
		super.stop();
		executor.shutdownNow();
	}

	public Status process() throws EventDeliveryException {
		LOG.info("start processing events");
		try {

			for (String table : models) {
				LOG.info("Starting worker thread for table [" + table +"]");
				HibernateHandler handler = new HibernateHandler(context, getChannelProcessor(), table);
				executor.execute(handler);
				
			}
			
			Thread.sleep(pollInterval);
			return Status.READY;

		} catch (Exception e) {
			LOG.error("Error procesing", e);
			return Status.BACKOFF;
		}

	}

	public long getBackOffSleepIncrement() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMaxBackOffSleepInterval() {
		// TODO Auto-generated method stub
		return 0;
	}

}
