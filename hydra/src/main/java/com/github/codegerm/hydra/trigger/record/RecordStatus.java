package com.github.codegerm.hydra.trigger.record;

import java.util.Properties;

import com.github.codegerm.hydra.trigger.TriggerStatus;

public class RecordStatus extends TriggerStatus {

	protected static final String KEY_RECORD_COUNT = "record.value";

	protected String recordValue;

	public RecordStatus() {
	}

	public RecordStatus(boolean triggered, String recordValue) {
		this.triggered = triggered;
		this.recordValue = recordValue;
	}

	@Override
	protected void readProperties(Properties prop) {
		super.readProperties(prop);
		this.recordValue = prop.getProperty(KEY_RECORD_COUNT);
	}

	@Override
	protected void writeProperties(Properties prop) {
		super.writeProperties(prop);
		prop.setProperty(KEY_RECORD_COUNT, recordValue == null ? "" : recordValue);
	}

	public String getRecordValue() {
		return recordValue;
	}

	public void setRecordValue(String recordValue) {
		this.recordValue = recordValue;
	}

}
