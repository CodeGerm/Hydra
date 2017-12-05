package com.github.codegerm.hydra.trigger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
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
		File file = new File(fileName);
		if (file.exists()) {
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(file);
				prop.load(fis);
				readProperties(prop);
			} catch (IOException e) {
				logger.warn("Cannot read status file", e);
			} finally {
				IOUtils.closeQuietly(fis);
			}
		} else {
			logger.info("Status file is not found, create a new one.");
			try {
				file.createNewFile();
			} catch (IOException e) {
				logger.warn("Cannot create status file", e);
			}
		}
	}

	public void save(String fileName) {
		Properties prop = new Properties();
		writeProperties(prop);

		File file = new File(fileName);
		if (!file.exists()) {
			try {
				FileUtils.forceMkdir(file.getParentFile());
				file.createNewFile();
			} catch (IOException e) {
				logger.warn("Cannot create status file", e);
			}
		}
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(file);
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
