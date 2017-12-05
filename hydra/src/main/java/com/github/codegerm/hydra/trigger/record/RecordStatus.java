package com.github.codegerm.hydra.trigger.record;

import java.util.Properties;

import com.github.codegerm.hydra.trigger.TriggerStatus;

public class RecordStatus extends TriggerStatus {

	protected static final String KEY_RECORD_COUNT = "record.count";
	protected static final String KEY_PRIMARY_KEY_VALUE = "record.pk.value";

	protected int recordCount;
	protected String primaryKeyValue;

	public RecordStatus() {
	}

	public RecordStatus(boolean triggered, int recordCount, String primaryKeyValue) {
		this.triggered = triggered;
		this.recordCount = recordCount;
		this.primaryKeyValue = primaryKeyValue;
	}

	public int getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}

	public String getPrimaryKeyValue() {
		return primaryKeyValue;
	}

	public void setPrimaryKeyValue(String primaryKeyValue) {
		this.primaryKeyValue = primaryKeyValue;
	}

	@Override
	protected void readProperties(Properties prop) {
		super.readProperties(prop);
		this.recordCount = Integer.parseInt(prop.getProperty(KEY_RECORD_COUNT, "0"));
		this.primaryKeyValue = prop.getProperty(KEY_PRIMARY_KEY_VALUE);
	}

	@Override
	protected void writeProperties(Properties prop) {
		super.writeProperties(prop);
		prop.setProperty(KEY_RECORD_COUNT, Integer.toString(recordCount));
		prop.setProperty(KEY_PRIMARY_KEY_VALUE, primaryKeyValue == null ? "" : primaryKeyValue);
	}

}
