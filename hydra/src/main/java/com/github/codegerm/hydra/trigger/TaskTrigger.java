package com.github.codegerm.hydra.trigger;

import org.apache.flume.Context;

public interface TaskTrigger {

	interface Action {
		void doAction();
	}

	void configure(Context context);

	void start();

	void stop();

	void addTriggerAction(Action action);

	void removeTriggerAction(Action action);

	void addDefaultTriggerAction();
	
	void removeDefaultTriggerAction();

}
