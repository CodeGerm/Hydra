package com.github.codegerm.hydra.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class HandlerFactory {
	private static final Logger logger = LoggerFactory.getLogger(HandlerFactory.class);
	
	
	public static AbstractHandler createHandler(String type) {
		if (Strings.isNullOrEmpty(type)) {
			return null;
		}
		try {
			Class<?> handlerClass = Class.forName(type);
			Object handler = handlerClass.newInstance();
			if (handler instanceof AbstractHandler) {
				return (AbstractHandler) handler;
			}
		} catch (Exception e) {
			logger.warn("Cannot create handler", e);
		}
		return null;
	}


}
