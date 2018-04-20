package com.github.codegerm.hydra.task;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


/**
 * @author yufan.liu
 *
 */
public class TaskRegister {
	
	private static final TaskRegister instance = new  TaskRegister();
	private static final int DEFAULT_QUEUE_SIZE = 1024;
	private BlockingQueue<Task> taskQueue;
	private BlockingQueue<Result> resultQueue;
	private Map<String, Task> runningTasks;
	
	
	
	public TaskRegister() {
		taskQueue = new ArrayBlockingQueue<Task>(DEFAULT_QUEUE_SIZE);
		resultQueue = new ArrayBlockingQueue<Result>(DEFAULT_QUEUE_SIZE);
		runningTasks = new HashMap<>();
	}
	
	public static TaskRegister getInstance(){
		return instance;
	}
	
	
	
	/**
	 * @param task
	 */
	public void addTask(Task task){
		taskQueue.add(task);
	}
	
	public Task getTaskByPoll(){
		return taskQueue.poll();			
	}
	
	public Task getTaskByTake() throws InterruptedException{
		return taskQueue.take();
	}
	
	public void cleanTaskQueue(){
		taskQueue.clear();
	}
	
	public void assignSnapshotId(String snapshotId, Task task) {
		runningTasks.put(snapshotId, task);
	}
	
	public void markTaskDone(String snapshotId) {
		runningTasks.remove(snapshotId);
	}
	
	public void addResult(Result result){
		resultQueue.add(result);		
	}
	
	public Result getResultByPoll(){
		return resultQueue.poll();
	}
	
	public Result getResultByTake() throws InterruptedException{
		return resultQueue.take();
	}
	
	public boolean isAnyTaskRunningOrPending() {
		if (taskQueue.size() > 0) {
			return true;
		}
		return runningTasks.size() > 0;		
	}

}
