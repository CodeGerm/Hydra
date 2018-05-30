package com.github.codegerm.hydra.trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.source.SqlSourceUtil;
import com.github.codegerm.hydra.task.Task;
import com.github.codegerm.hydra.task.TaskRegister;
import com.github.codegerm.hydra.task.TaskRegisterFactory;
import com.github.codegerm.hydra.utils.AvroSchemaUtils;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public abstract class AbstractTaskTrigger implements TaskTrigger {

	public static final String KEY_TRIGGER_PARAMS = "trigger.parameters";

	private static final Logger LOG = LoggerFactory.getLogger(AbstractTaskTrigger.class);
	

	protected Context context;
	protected List<Action> actions = new ArrayList<>();
	protected Action defaultAction;
	private TaskRegister register;

	@Override
	public void configure(Context context) {
		this.context = context;
		String taskQueueId =  context.getString(SqlSourceUtil.TASK_QUEUE_ID);
		if(taskQueueId == null){
			LOG.info("No task queue id defined, use default queue");
			register = TaskRegister.getInstance();
		} else {
			LOG.info("Task queue id: " + taskQueueId );
			register = TaskRegisterFactory.getInstance().getPutInstance(taskQueueId);
		}
	}

	@Override
	public void addTriggerAction(Action action) {
		if (!actions.contains(action)) {
			actions.add(action);
		}
	}

	@Override
	public void removeTriggerAction(Action action) {
		actions.remove(action);
	}

	@Override
	public void addDefaultTriggerAction() {
		if (defaultAction != null) {
			removeTriggerAction(defaultAction);
		}
		defaultAction = new Action() {

			@Override
			public void doAction() {
				String instanceName = context.getString(SqlSourceUtil.MODEL_INSTANCE_KEY);
				String schemas = context.getString(SqlSourceUtil.MODEL_SCHEMA_KEY);
				if (instanceName == null) {
					LOG.warn("Model instance name defined in flume is empty, skip snapshot.");
					return;
				}
				if (Strings.isNullOrEmpty(schemas)) {
					LOG.warn("Schemas defined in flume is empty, skip snapshot.");
					return;
				}
				String replace = AvroSchemaUtils.getReplaceSchemas(context);
				Map<String, String> schemaMap = AvroSchemaUtils.getSchemasAsStringMap(schemas);
				schemaMap = AvroSchemaUtils.replaceSchemaNameByEnv(schemaMap, replace);
				if (schemaMap != null) {
					LOG.info("Start snapshot, task queued.");
					Task task = new Task(schemaMap, instanceName);
					Map <String, String> extraInfo = new HashMap<String, String>(); 
					extraInfo.put(SqlSourceUtil.CREATE_BY_KEY, SqlSourceUtil.CREATE_BY_SYSTEM);
					task.setExtraInfo(extraInfo);
					register.addTask(task);
				} else {
					LOG.warn("Schema format is invalid, skip snapshot.");
				}
			}

		};
		addTriggerAction(defaultAction);
	}

	@Override
	public void removeDefaultTriggerAction() {
		removeTriggerAction(defaultAction);
	}

	protected void triggerActions() {
		for (Action action : actions) {
			LOG.info("action: " + action);
			try {
				action.doAction();
			} catch (Exception e) {
				LOG.error("error in running action: ", e);
			}

		}
	}



}
