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

import org.apache.commons.lang3.EnumUtils;
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
import com.github.codegerm.hydra.handler.HibernateHandler;
import com.github.codegerm.hydra.source.SqlSourceUtil.MODE;
import com.github.codegerm.hydra.task.Result;
import com.github.codegerm.hydra.task.Task;
import com.github.codegerm.hydra.task.TaskRegister;
import com.github.codegerm.hydra.task.TaskRegisterFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author yufan.liu
 *
 */
public class SqlSource extends AbstractSource implements Configurable, PollableSource {

	private ExecutorService executor;
	private static final Logger LOG = LoggerFactory.getLogger(SqlSource.class);
	private Task task;
	private String modelId;
	private String snapshotId;
	private long pollInterval;
	private long timeout;
	private long serverTimeout;
	private Context context;
	private long backoffSleepIncrement;
	private long maxBackOffSleepInterval;
	private Gson gson = new GsonBuilder().create();
	protected Map<String, String> entitySchemas;
	private MODE mode;
	private String createBy;

	private TaskRegister register;
	/* (non-Javadoc)
	 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void configure(Context context) {
		LOG.info("Start configuring SqlSource");
		this.context = context;
		String taskQueueId =  context.getString(SqlSourceUtil.TASK_QUEUE_ID);
		if(taskQueueId == null){
			LOG.info("No task queue id defined, use default queue");
			register = TaskRegister.getInstance();
		} else {
			LOG.info("Task queue id: " + taskQueueId );
			register = TaskRegisterFactory.getInstance().getPutInstance(taskQueueId);
		}
		String modeString  = context.getString(SqlSourceUtil.MODE_KEY, SqlSourceUtil.DEFAULT_MODE);
		if(!EnumUtils.isValidEnum(MODE.class, modeString))
			throw new FlumeException("Mode: " + modeString + " is not supported");
		mode = MODE.valueOf(modeString);
		LOG.info("Running in ["+ mode +"] mode");

		int thread = context.getInteger(SqlSourceUtil.WORKER_THREAD_NUM_KEY, SqlSourceUtil.DEFAULT_THREAD_NUM);
		
		timeout = context.getLong(SqlSourceUtil.TIMEOUT_KEY, SqlSourceUtil.DEFAULT_TIMEOUT);
		serverTimeout = context.getLong(SqlSourceUtil.SERVER_TIMEOUT_KEY, SqlSourceUtil.DEFAULT_SERVER_TIMEOUT);
		executor = Executors.newFixedThreadPool(thread);

		backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
				PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
		maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
				PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);

		if(mode.equals(MODE.SCHEDULE)){
			if(!context.containsKey(SqlSourceUtil.MODEL_INSTANCE_KEY))
				throw new FlumeException("No model instance name defined");
			modelId = context.getString(SqlSourceUtil.MODEL_INSTANCE_KEY);
			
			if(!context.containsKey(SqlSourceUtil.MODEL_SCHEMA_KEY))
				throw new FlumeException("No model schema defined");
			String schema = context.getString(SqlSourceUtil.MODEL_SCHEMA_KEY);
			entitySchemas = gson.fromJson(schema, Map.class);
			
			pollInterval = context.getLong(SqlSourceUtil.POLL_INTERVAL_KEY, SqlSourceUtil.DEFAULT_SCHEDULE_POLL_INTERVAL);
		}
		
		if(mode.equals(MODE.TASK)){
			pollInterval = context.getLong(SqlSourceUtil.POLL_INTERVAL_KEY, SqlSourceUtil.DEFAULT_TASK_POLL_INTERVAL);
		}

	}

	/* (non-Javadoc)
	 * @see org.apache.flume.source.AbstractSource#start()
	 */
	@Override
	public void start() {
		super.start();
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.source.AbstractSource#stop()
	 */
	@Override
	public void stop() {
		LOG.info("Stopping SqlSource");
		super.stop();
		executor.shutdownNow();
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.PollableSource#process()
	 */
	@Override
	public Status process() throws EventDeliveryException {
		if(mode.equals(MODE.SCHEDULE)){
			snapshotId = modelId+System.currentTimeMillis();
			return execute();
		}
		else if(mode.equals(MODE.TASK)){
			task = TaskRegister.getInstance().getTaskByPoll();
			Status status = null;
			if(task == null){
				status = Status.READY;
			} else {
				setModelId(task.getModelId());
				setEntitySchemas(task.getEntitySchemas());
				Map<String, String> extraInfo = task.getExtraInfo();
				if(extraInfo!=null){
					setCreateBy(extraInfo.get(SqlSourceUtil.CREATE_BY_KEY));
				}
				status = execute();
			}			
			try {
				Thread.sleep(pollInterval);
			} catch (InterruptedException e) {
				LOG.error("Error in waiting", e);
			}
			return status;
		} else
			throw new FlumeException("Mode: " + mode + " is not supported");

	}
	

	/**
	 * @return
	 * @throws EventDeliveryException
	 */
	public Status execute() throws EventDeliveryException {
		LOG.info("start snapshot: " + snapshotId);
		if(entitySchemas == null){
			throw new FlumeException("Entity Schemas is not initiated");
		}
		TaskRegister.getInstance().assignSnapshotId(snapshotId, task);
		getChannelProcessor().processEvent(StatusEventBuilder.buildSnapshotBeginEvent(snapshotId, modelId, serverTimeout, createBy));
		try {

			List<Callable<Boolean>> taskList = new ArrayList<Callable<Boolean>>();
			for (Entry<String, String> entry:entitySchemas.entrySet()) {
				LOG.info("Starting worker thread for table [" + entry.getKey() + "]");
				HibernateHandler handler = new HibernateHandler(snapshotId, context, getChannelProcessor(), modelId, entry.getKey(), entry.getValue());
				taskList.add(handler);
			}
			List<Future<Boolean>> result = executor.invokeAll(taskList, timeout, TimeUnit.MILLISECONDS);
			//TODO: handle exceptions in result
			getChannelProcessor().processEvent(StatusEventBuilder.buildSnapshotEndEvent(snapshotId, modelId));
			if(mode.equals(MODE.TASK)){
				Result runningResult = new Result(snapshotId, result);
				register.addResult(runningResult);
				register.markTaskDone(snapshotId);
			} else
				Thread.sleep(pollInterval);
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

	public void setEntitySchemas(Map<String, String> entitySchemas){
		this.entitySchemas = entitySchemas;
	}

	public void setModelId(String modelId){
		this.modelId = modelId;
	}
	
	public void setCreateBy(String createBy) {
		this.createBy = createBy;
	}

}
