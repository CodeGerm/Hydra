package com.github.codegerm.hydra.trigger.record;

public class NewRecordTrigger extends SqlResultTrigger {

	private static final String SQL_SELECT_COUNT = "select count(*) from ";

	@Override
	protected String generateSQL(String parameters) {
		return SQL_SELECT_COUNT + parameters;
	}

}
