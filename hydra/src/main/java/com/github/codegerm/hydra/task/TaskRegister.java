package com.github.codegerm.hydra.task;


import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author yufan.li
 *
 */
public class TaskRegister {
	
	private static final TaskRegister instance = new  TaskRegister();
	private static final int DEFAULT_QUEUE_SIZE = 1024;
	private BlockingQueue<Task> taskQueue;
	private BlockingQueue<Result> resultQueue;
	
	private  TaskRegister() {
		taskQueue = new ArrayBlockingQueue<Task>(DEFAULT_QUEUE_SIZE);
		resultQueue = new ArrayBlockingQueue<Result>(DEFAULT_QUEUE_SIZE);
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
	
	public void addResult(Result result){
		resultQueue.add(result);
	}
	
	public Result getResultByPoll(){
		return resultQueue.poll();
	}
	
	public Result getResultByTake() throws InterruptedException{
		return resultQueue.take();
	}
	

	
	

	


}
