package com.github.codegerm.hydra.source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.trigger.TaskTrigger;
import com.github.codegerm.hydra.trigger.TriggerFactory;

/**
 * @author yufan.li
 *
 */
public class SqlEventDrivenSource extends AbstractSource implements EventDrivenSource, Configurable {

	private static final Logger LOG = LoggerFactory.getLogger(SqlEventDrivenSource.class);

	protected TaskRunner taskRunner;
	protected TaskTrigger taskTrigger;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		LOG.info("Start configuring SqlEventDrivenSource");

		taskRunner = new TaskRunner(getChannelProcessor());
		taskRunner.configure(context);

		taskTrigger = TriggerFactory.createTrigger(context.getString(SqlSourceUtil.TRIGGER_TYPE_KEY));
		if (taskTrigger != null) {
			LOG.info("Trigger is enabled");
			taskTrigger.configure(context);
			taskTrigger.addDefaultTriggerAction();
		} else {
			LOG.info("Trigger is disabled");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.source.AbstractSource#start()
	 */
	@Override
	public synchronized void start() {
		if (taskRunner != null) {
			taskRunner.start();
		}
		if (taskTrigger != null) {
			taskTrigger.start();
		}
		super.start();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.source.AbstractSource#stop()
	 */
	@Override
	public synchronized void stop() {
		if (taskTrigger != null) {
			taskTrigger.stop();
		}
		if (taskRunner != null) {
			taskRunner.stop();
		}
		super.stop();
	}

}
