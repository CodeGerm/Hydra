package com.github.codegerm.hydra.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.event.StatusEventBuilder;

public class SqlSource extends AbstractSource implements Configurable, PollableSource {

	private ExecutorService executor;
	private static final Logger LOG = LoggerFactory.getLogger(SqlSource.class);
	private List<String> models;
	private String modelId;
	private String snapshotId;
	private static final int DEFAULT_THREAD_NUM = 4;
	private static final long DEFAULT_POLL_INTERVAL = 100000;
	private static final long DEFAULT_TIMEOUT = 100000;
	private long pollInterval;
	private long timeout;
	private Context context;
	private long backoffSleepIncrement;
	private long maxBackOffSleepInterval;

	@Override
	public void configure(Context context) {
		LOG.info("Start configuring SqlSource");
		this.context = context;
		models = new ArrayList<String>();
		if (context.containsKey("table")) {
			String content[] = context.getString(SqlSourceUtil.TABLE_KEY).split(",");
			models.addAll(Arrays.asList(content));
		}
		int thread = context.getInteger(SqlSourceUtil.WORKER_THREAD_NUM, DEFAULT_THREAD_NUM);
		pollInterval = context.getLong(SqlSourceUtil.POLL_INTERVAL_KEY, DEFAULT_POLL_INTERVAL);
		timeout = context.getLong(SqlSourceUtil.TIMEOUT_KEY, DEFAULT_TIMEOUT);
		executor = Executors.newFixedThreadPool(thread);

		backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
				PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
		maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
				PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
		
		if(!context.containsKey(SqlSourceUtil.MODEL_ID_KEY))
			throw new FlumeException("No model id defined");
		modelId = context.getString(SqlSourceUtil.MODEL_ID_KEY);
		snapshotId = modelId+System.currentTimeMillis();
	}

	@Override
	public void start() {
		super.start();
	}

	@Override
	public void stop() {
		LOG.info("Stopping SqlSource");
		super.stop();
		executor.shutdownNow();
	}

	@Override
	public Status process() throws EventDeliveryException {
		LOG.info("start processing events");
		getChannelProcessor().processEvent(StatusEventBuilder.buildSnapshotBeginEvent(snapshotId));
		try {

			List<Callable<Boolean>> taskList = new ArrayList<Callable<Boolean>>();
			for (String table : models) {
				LOG.info("Starting worker thread for table [" + table + "]");
				HibernateHandler handler = new HibernateHandler(snapshotId, context, getChannelProcessor(), table);
				taskList.add(handler);
			}
			List<Future<Boolean>> result = executor.invokeAll(taskList, timeout, TimeUnit.MILLISECONDS);
			Thread.sleep(pollInterval);
			
			getChannelProcessor().processEvent(StatusEventBuilder.buildSnapshotEndEvent(snapshotId));
			return Status.READY;

		} catch (Exception e) {
			LOG.error("Error procesing", e);
			return Status.BACKOFF;
		}

	}

	@Override
	public long getBackOffSleepIncrement() {
		return backoffSleepIncrement;
	}

	@Override
	public long getMaxBackOffSleepInterval() {
		return maxBackOffSleepInterval;
	}
	
	protected Boolean validateResult(){
		return false;
	}

}
