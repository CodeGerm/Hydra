package com.github.codegerm.hydra.schema;

import java.util.ArrayList;
import java.util.List;

public class ModelSchema {

	private String id;
	private List<TableSchema> tables;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public List<TableSchema> getTables() {
		return tables;
	}
	public void setTables(List<TableSchema> tables) {
		this.tables = tables;
	}
	public ModelSchema(String id, List<TableSchema> tables) {
		super();
		this.id = id;
		this.tables = tables;
	}

	public List<String> getTableNames(){
		List<String> tableNames = new ArrayList<String>();
		for(TableSchema table:tables){
			tableNames.add(table.getName());
		}
		return tableNames;
	}


}
