package com.github.codegerm.hydra.writer;

import java.util.List;

public interface RecordWriter {

	void writeAll(List<List<Object>> records);
	
	void writeAllString(List<String> records);

	void flush();

	void close();

}
