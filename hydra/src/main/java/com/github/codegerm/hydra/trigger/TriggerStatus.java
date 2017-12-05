package com.github.codegerm.hydra.trigger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerStatus {

	private static final Logger logger = LoggerFactory.getLogger(TriggerStatus.class);

	protected boolean triggered;

	public boolean isTriggered() {
		return triggered;
	}

	public void setTriggered(boolean triggered) {
		this.triggered = triggered;
	}

	public void load(String fileName) {
		Properties prop = new Properties();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(new File(fileName));
			prop.load(fis);
			readProperties(prop);
		} catch (IOException e) {
			logger.warn("Cannot read status file", e);
		} finally {
			IOUtils.closeQuietly(fis);
		}
	}

	public void save(String fileName) {
		Properties prop = new Properties();
		writeProperties(prop);

		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(new File(fileName));
			prop.store(fos, null);
		} catch (IOException e) {
			logger.warn("Cannot write status file", e);
		} finally {
			IOUtils.closeQuietly(fos);
		}
	}

	protected void readProperties(Properties prop) {
	}

	protected void writeProperties(Properties prop) {
	}

}
