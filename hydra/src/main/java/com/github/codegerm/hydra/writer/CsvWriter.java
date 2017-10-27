package com.github.codegerm.hydra.writer;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.event.SqlEventBuilder;
import com.opencsv.CSVWriter;

public class CsvWriter implements RecordWriter{
	
	private static final Logger LOG = LoggerFactory.getLogger(CsvWriter.class);
	public static final String WRITER_TYPE = "csv";

	private CSVWriter openCsvWriter; 
	private Map<String, String> header;
	private String entityName;
	
	public CsvWriter(ChannelProcessor processor, char separator, String entitySchema) {
		openCsvWriter = new CSVWriter(new ChannelWriter(processor), separator);
		header = new HashMap<String, String>();
		header.put(WRITER_TYPE_KEY, WRITER_TYPE);
		String entityName = AvroRecordUtil.getSchemaName(entitySchema);
		if(entityName!=null)
			header.put(ENTITY_NAME_KEY, entityName);
	}
	
	@Override
	public void flush() {
		try {
			openCsvWriter.flush();
		} catch (IOException e) {
			LOG.error("Error flushing events: ", e);
		}
	}
	
	@Override
	public void close() {
		try {
			openCsvWriter.close();
		} catch (IOException e) {
			LOG.error("Error closing writer: ", e);
		}
	}
	
	public void writeAll(List<String[]> arg0){
		openCsvWriter.writeAll(arg0, true);
	}

	
	private class ChannelWriter extends Writer {
		private List<Event> events = new ArrayList<>();
		private ChannelProcessor processor;

		public ChannelWriter(ChannelProcessor processor) {
			this.processor = processor;
		}

		@Override
		public void write(char[] cbuf, int off, int len) throws IOException {

			String s = new String(cbuf);
			Event event = SqlEventBuilder.build(s.substring(off, len - 1).getBytes(), header);
			events.add(event);

		}

		@Override
		public void flush() throws IOException {

			if (events != null && !events.isEmpty()) {
				LOG.info("Flushing: " + events.size() + " event(s): ");
				processor.processEventBatch(events);
			}

			events.clear();
		}

		@Override
		public void close() throws IOException {
			flush();
		}

	}



}
