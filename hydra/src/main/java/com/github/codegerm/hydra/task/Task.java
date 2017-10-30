package com.github.codegerm.hydra.task;

import java.util.Map;

public class Task {
	
	private Map<String, String> entitySchemas;
	private String modelId;
	public Map<String, String> getEntitySchemas() {
		return entitySchemas;
	}
	public void setEntitySchemas(Map<String, String> entitySchemas) {
		this.entitySchemas = entitySchemas;
	}
	public String getModelId() {
		return modelId;
	}
	public void setModelId(String modelId) {
		this.modelId = modelId;
	}
	public Task(Map<String, String> entitySchemas, String modelId) {
		this.entitySchemas = entitySchemas;
		this.modelId = modelId;
	}
	@Override
	public String toString() {
		return "Task [entitySchemas=" + entitySchemas + ", modelId=" + modelId + "]";
	}
	
}