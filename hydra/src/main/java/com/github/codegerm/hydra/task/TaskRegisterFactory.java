package com.github.codegerm.hydra.task;

import java.util.concurrent.ConcurrentHashMap;

public class TaskRegisterFactory {
	
	
	private  ConcurrentHashMap<String, TaskRegister> instances;
	private static final TaskRegisterFactory factory = new TaskRegisterFactory();
	
	public static TaskRegisterFactory getInstance(){
		return factory;
	}
	
	private TaskRegisterFactory(){
		instances = new ConcurrentHashMap<String, TaskRegister>();
	}
	
	public synchronized TaskRegister addInstance(String key){
		TaskRegister newInstance = new TaskRegister();
		instances.put(key, newInstance);
		return newInstance;
	}
	
	public synchronized TaskRegister getPutInstance(String key){
		if(!instances.containsKey(key)){
			return addInstance(key);
		} else
			return instances.get(key);
	}

}
