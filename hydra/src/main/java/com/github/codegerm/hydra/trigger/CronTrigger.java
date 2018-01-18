package com.github.codegerm.hydra.trigger;

import java.util.Date;

import org.apache.flume.Context;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CronTrigger extends AbstractTaskTrigger {

	public static class RunnableJob implements Job {

		public static final String KEY_RUNNABLE = "runnable";

		@Override
		public void execute(JobExecutionContext context) throws JobExecutionException {
			Runnable task = (Runnable) context.getMergedJobDataMap().get(KEY_RUNNABLE);
			if (task != null) {
				task.run();
			} else {
				logger.warn("Runnable job is null");
			}
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(CronTrigger.class);

	private String cronExp;
	private Scheduler scheduler;

	@Override
	public void configure(Context context) {
		super.configure(context);

		this.cronExp = context.getString(KEY_TRIGGER_PARAMS);
	}

	@Override
	public void start() {
		boolean valid = CronExpression.isValidExpression(cronExp);
		if (!valid) {
			logger.warn("Cron expression is not valid");
			return;
		}

		SchedulerFactory sf = new StdSchedulerFactory();
		try {
			scheduler = sf.getScheduler();
			scheduler.start();
			scheduler.scheduleJob(createJob(), createTrigger());

		} catch (SchedulerException e) {
			logger.warn("Start scheduler failed", e);
		}
	}

	@Override
	public void stop() {
		if (scheduler != null) {
			try {
				scheduler.shutdown(true);
			} catch (SchedulerException e) {
				logger.warn("Shutdown scheduler failed", e);
			}
		}
	}

	private JobDetail createJob() {
		JobDataMap data = new JobDataMap();
		data.put(RunnableJob.KEY_RUNNABLE, new Runnable() {

			@Override
			public void run() {
				logger.info("Job triggered at " + new Date().toString());
				triggerActions();
			}
		});
		JobDetail job = JobBuilder.newJob(RunnableJob.class).usingJobData(data).build();
		return job;
	}

	private Trigger createTrigger() {
		return TriggerBuilder.newTrigger() //
				.startNow() //
				.withSchedule(CronScheduleBuilder.cronSchedule(cronExp)) //
				.build();

	}

}
