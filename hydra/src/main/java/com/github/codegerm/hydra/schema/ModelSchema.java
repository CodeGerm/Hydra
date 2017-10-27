package com.github.codegerm.hydra.schema;

import java.util.ArrayList;
import java.util.List;

public class ModelSchema {

	private String name;
	private List<EntitySchema> tables;
	public String getId() {
		return name;
	}
	public void setName(String id) {
		this.name = id;
	}
	public List<EntitySchema> getTables() {
		return tables;
	}
	public void setTables(List<EntitySchema> tables) {
		this.tables = tables;
	}
	public ModelSchema(String id, List<EntitySchema> tables) {
		super();
		this.name = id;
		this.tables = tables;
	}

	public List<String> getTableNames(){
		List<String> tableNames = new ArrayList<String>();
		for(EntitySchema table:tables){
			tableNames.add(table.getName());
		}
		return tableNames;
	}


}
