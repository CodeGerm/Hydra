package com.github.codegerm.hydra.trigger.record;

import com.github.codegerm.hydra.utils.AvroSchemaUtils;

public class NewRecordTrigger extends SqlResultTrigger {

	private static final String SQL_SELECT_COUNT = "select count(*) from ";

	@Override
	protected String generateSQL(String parameters) {
        //replace the schema
		String replace = AvroSchemaUtils.getReplaceTriggerTable(context);
		if(replace!=null){
			parameters = AvroSchemaUtils.replaceTableNameOfTableByEnv(parameters, replace);
		}
		return SQL_SELECT_COUNT + parameters;
	}

}
