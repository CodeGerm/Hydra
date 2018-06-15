package com.github.codegerm.hydra.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class PreProcessorFactory {
	
	private static final Logger logger = LoggerFactory.getLogger(PreProcessorFactory.class);
	
	public static PreProcessor createProcessor(String type) {
		if (Strings.isNullOrEmpty(type)) {
			return null;
		}
		try {
			Class<?> handlerClass = Class.forName(type);
			Object handler = handlerClass.newInstance();
			if (handler instanceof PreProcessor) {
				return (PreProcessor) handler;
			} else
				throw new IllegalArgumentException("Cannot create pre-processor: " + type);
		} catch (Exception e) {
			logger.error("Cannot create pre-processor: " + type, e);
			throw new IllegalArgumentException(e);
		}
	}

}
