package com.github.codegerm.hydra.schema;

import java.util.ArrayList;
import java.util.List;

public class EntitySchema {
	
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
	public EntitySchema(String name, List<ColumnSchema> columns) {
		super();
		this.name = name;
		this.columns = columns;
	}
	
	public List<String> getColumnNames(){
		List<String> columnNames = new ArrayList<String>();
		for(ColumnSchema column:columns){
			columnNames.add(column.getName());
		}
		return columnNames;
	}

}
