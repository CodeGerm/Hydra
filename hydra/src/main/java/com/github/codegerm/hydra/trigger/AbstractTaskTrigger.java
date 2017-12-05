package com.github.codegerm.hydra.trigger;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractTaskTrigger implements TaskTrigger {

	protected List<Action> actions = new ArrayList<>();

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

	protected void triggerActions() {
		for (Action action : actions) {
			action.doAction();
		}
	}

}
