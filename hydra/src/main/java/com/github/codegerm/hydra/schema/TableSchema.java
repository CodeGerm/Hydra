package com.github.codegerm.hydra.schema;

import java.util.List;

public class TableSchema {
	
	private String name;	
	List<ColumnSchema> columns;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<ColumnSchema> getColumns() {
		return columns;
	}
	public void setColumns(List<ColumnSchema> columns) {
		this.columns = columns;
	}
	public TableSchema(String name, List<ColumnSchema> columns) {
		super();
		this.name = name;
		this.columns = columns;
	}

}
