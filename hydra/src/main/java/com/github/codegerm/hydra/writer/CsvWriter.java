package com.github.codegerm.hydra.writer;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.event.SqlEventBuilder;
import com.opencsv.CSVWriter;

public class CsvWriter {
	
	private static final Logger LOG = LoggerFactory.getLogger(CsvWriter.class);

	private CSVWriter openCsvWriter; 
	
	public CsvWriter(ChannelProcessor processor, char separator) {
		openCsvWriter = new CSVWriter(new ChannelWriter(processor), separator);
	}
	
	public void flush() throws IOException{
		openCsvWriter.flush();
	}
	
	public void close() throws IOException{
		openCsvWriter.close();
	}
	
	public void writeAll(List<String[]> arg0, boolean arg1){
		openCsvWriter.writeAll(arg0, arg1);
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
			Event event = SqlEventBuilder.build(s.substring(off, len - 1).getBytes());
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
