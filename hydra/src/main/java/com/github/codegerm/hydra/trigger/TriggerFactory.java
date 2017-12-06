package com.github.codegerm.hydra.trigger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerFactory {

	private static final Logger logger = LoggerFactory.getLogger(TriggerFactory.class);

	public static TaskTrigger createTrigger(String type) {
		try {
			Class<?> triggerClass = Class.forName(type);
			Object trigger = triggerClass.newInstance();
			if (trigger instanceof TaskTrigger) {
				return (TaskTrigger) trigger;
			}
		} catch (Exception e) {
			logger.warn("Cannot create trigger", e);
		}
		return null;
	}

}
