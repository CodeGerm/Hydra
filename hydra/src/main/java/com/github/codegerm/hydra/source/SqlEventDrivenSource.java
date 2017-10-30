package com.github.codegerm.hydra.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.event.StatusEventBuilder;
import com.github.codegerm.hydra.handler.HibernateHandler;
import com.github.codegerm.hydra.task.Result;
import com.github.codegerm.hydra.task.Task;
import com.github.codegerm.hydra.task.TaskRegister;

public class SqlEventDrivenSource extends AbstractSource implements EventDrivenSource, Configurable {

	private ExecutorService executor;
	private ExecutorService mainExecutor;
	private static final Logger LOG = LoggerFactory.getLogger(SqlEventDrivenSource.class);
	private SqlRunnable runner;
	private long timeout;
	private Context context;

	@Override
	public void configure(Context context) {
		LOG.info("Start configuring SqlEventDrivenSource");
		this.context = context;

		int thread = context.getInteger(SqlSourceUtil.WORKER_THREAD_NUM_KEY, SqlSourceUtil.DEFAULT_THREAD_NUM);

		timeout = context.getLong(SqlSourceUtil.TIMEOUT_KEY, SqlSourceUtil.DEFAULT_TIMEOUT);
		executor = Executors.newFixedThreadPool(thread);

		mainExecutor = Executors.newSingleThreadExecutor();

	}

	@Override
	public synchronized void start() {
		runner = new SqlRunnable();
		mainExecutor.execute(runner);
		super.start();
	}

	@Override
	public synchronized void stop() {
		
		executor.shutdown();

		while (!executor.isTerminated()) {
			LOG.debug("Waiting for exec executor service to stop");
			try {
				executor.awaitTermination(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				LOG.debug("Interrupted while waiting for exec executor service "
						+ "to stop. Just exiting.");
				Thread.currentThread().interrupt();
			}
		}
		super.stop();
	}

	private class SqlRunnable implements Runnable {

		private String modelId;
		private String snapshotId;
		protected Map<String, String> entitySchemas;

		public void setEntitySchemas(Map<String, String> entitySchemas) {
			this.entitySchemas = entitySchemas;
		}

		public void setModelId(String modelId) {
			this.modelId = modelId;
		}

		@Override
		public void run() {
			try {
				while (true) {
					Task task = TaskRegister.getInstance().getTaskByTake();
					setModelId(task.getModelId());
					setEntitySchemas(task.getEntitySchemas());
					execute();
				}
			} catch (Exception e) {
				Thread.currentThread().interrupt();
			}
		}

		public Boolean execute() {
			snapshotId = modelId + System.currentTimeMillis();
			LOG.info("start snapshot: " + snapshotId);
			if (entitySchemas == null) {
				throw new FlumeException("Entity Schemas is not initiated");
			}
			getChannelProcessor().processEvent(StatusEventBuilder.buildSnapshotBeginEvent(snapshotId));
			try {

				List<Callable<Boolean>> taskList = new ArrayList<Callable<Boolean>>();
				for (Entry<String, String> entry : entitySchemas.entrySet()) {
					LOG.info("Starting worker thread for table [" + entry.getKey() + "]");
					HibernateHandler handler = new HibernateHandler(snapshotId, context, getChannelProcessor(),
							entry.getKey(), entry.getValue());
					taskList.add(handler);
				}
				List<Future<Boolean>> result = executor.invokeAll(taskList, timeout, TimeUnit.MILLISECONDS);
				// TODO: handle exceptions in result
				getChannelProcessor().processEvent(StatusEventBuilder.buildSnapshotEndEvent(snapshotId));

				Result runningResult = new Result(snapshotId, result);
				TaskRegister.getInstance().addResult(runningResult);

				return true;

			} catch (Exception e) {
				LOG.error("Error procesing", e);
				return false;
			}
		}

	


	}

}
