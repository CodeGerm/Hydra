package com.github.codegerm.hydra.writer;

import java.util.List;

public interface RecordWriter {
	
	public static final String WRITER_TYPE_KEY = "writer.type";
	public static final String ENTITY_NAME_KEY = "entity.name";
	
	
	public void close();
	
	public void flush();

	
}
