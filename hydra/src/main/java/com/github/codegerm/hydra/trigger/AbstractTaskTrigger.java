package com.github.codegerm.hydra.trigger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.source.SqlSourceUtil;
import com.github.codegerm.hydra.task.Task;
import com.github.codegerm.hydra.task.TaskRegister;
import com.github.codegerm.hydra.utils.AvroSchemaUtils;
import com.google.common.base.Strings;

public abstract class AbstractTaskTrigger implements TaskTrigger {

	public static final String KEY_TRIGGER_PARAMS = "trigger.parameters";
	
	private static final Logger LOG = LoggerFactory.getLogger(AbstractTaskTrigger.class);

	protected Context context;
	protected List<Action> actions = new ArrayList<>();
	protected Action defaultAction;

	@Override
	public void configure(Context context) {
		this.context = context;
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
				String modelMapName = context.getString(SqlSourceUtil.MODELMAP_NAME_KEY);
				String schemas = context.getString(SqlSourceUtil.MODEL_SCHEMA_KEY);
				if (modelMapName == null) {
					LOG.warn("Model name defined in flume is empty, skip snapshot.");
					return;
				}
				if (Strings.isNullOrEmpty(schemas)) {
					LOG.warn("Schemas defined in flume is empty, skip snapshot.");
					return;
				}

				Map<String, String> schemaMap = AvroSchemaUtils.getSchemasAsStringMap(schemas);
				if (schemaMap != null) {
					LOG.info("Start snapshot, task queued.");
					TaskRegister.getInstance().addTask(new Task(schemaMap, modelMapName));
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
			try{
				action.doAction();
			} catch (Exception e){
				LOG.error("error in running action: ", e);
			}
			
		}
	}

}
