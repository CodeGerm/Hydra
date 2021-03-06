package com.github.codegerm.hydra.task;

import java.util.Map;

public class Task {
	
	private Map<String, String> entitySchemas;
	private String modelId;
	private Map<String, String> extraInfo;
	private Boolean killSignal = false;
	
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
	
	public Task(Boolean KillSignal) {
		this.killSignal = KillSignal;
	}
	
	public Boolean getKillSignal() {
		return killSignal;
	}
	public Map<String, String> getExtraInfo() {
		return extraInfo;
	}
	public void setExtraInfo(Map<String, String> extraInfo) {
		this.extraInfo = extraInfo;
	}
	@Override
	public String toString() {
		return "Task [entitySchemas=" + entitySchemas + ", modelId=" + modelId + ", extraInfo=" + extraInfo
				+ ", killSignal=" + killSignal + "]";
	}
	
	
}