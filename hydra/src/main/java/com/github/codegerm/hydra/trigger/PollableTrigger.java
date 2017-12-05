package com.github.codegerm.hydra.trigger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PollableTrigger extends AbstractTaskTrigger {

	public static final String KEY_INIT_DELAY = "initDelay";
	public static final String KEY_POLL_INTERVAL = "pollInterval";

	public static final long DEFAULT_INIT_DELAY = 1000;
	public static final long DEFAULT_POLL_INTERVAL = 5000;

	private static final Logger logger = LoggerFactory.getLogger(PollableTrigger.class);

	private long initDelay;
	private long pollInterval;
	private ScheduledExecutorService executor;

	@Override
	public void configure(Context context) {
		initDelay = context.getLong(KEY_INIT_DELAY, DEFAULT_INIT_DELAY);
		pollInterval = context.getLong(KEY_POLL_INTERVAL, DEFAULT_POLL_INTERVAL);
		executor = Executors.newScheduledThreadPool(1);
	}

	@Override
	public void start() {
		executor.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				process();
			}
		}, initDelay, pollInterval, TimeUnit.MILLISECONDS);
	}

	@Override
	public void stop() {
		executor.shutdown();
		while (!executor.isTerminated()) {
			logger.debug("Waiting for exec executor service to stop");
			try {
				executor.awaitTermination(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				logger.debug("Interrupted while waiting for exec executor service " + "to stop. Just exiting.");
				Thread.currentThread().interrupt();
			}
		}
	}

	protected abstract void process();

}
