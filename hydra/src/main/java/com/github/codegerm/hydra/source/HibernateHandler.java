package com.github.codegerm.hydra.source;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.reader.HibernateContext;
import com.github.codegerm.hydra.reader.HibernateReader;
import com.opencsv.CSVWriter;

public class HibernateHandler implements Callable<Boolean> {

	private Context context;
	private ChannelProcessor processor;
	protected HibernateContext jdbcContext;
	private CSVWriter csvWriter;
	private HibernateReader hibernateReader;
	private static final String DEFAULT_STATUS_DIRECTORY = "flume/jdbcSource/status";
	
	private static final Logger LOG = LoggerFactory.getLogger(HibernateHandler.class);
	private String status_file_path;
	private String table;
	
	@Override
	public Boolean call() {

		try {
	
			List<List<Object>> result = hibernateReader.executeQuery();
			System.out.println(result);
			if (!result.isEmpty()) {
				csvWriter.writeAll(jdbcContext.getAllRows(result), true);
				csvWriter.flush();
				jdbcContext.updateStatusFile();
			}

		} catch (IOException | InterruptedException e) {
			LOG.error("Error procesing row", e);
			close();
			return false;
		}
		close();
		return true;
	}

	public HibernateHandler(Context context, ChannelProcessor processor, String table) {
		this.context = context;
		this.processor = processor;
		this.table = table;
		configure();
	}

	public void configure() {
		LOG.getName();

		LOG.info("Reading and processing configuration values for source " + LOG.getName());
		status_file_path = context.getString(SqlSourceUtil.STATUS_DIRECTORY_KEY, DEFAULT_STATUS_DIRECTORY);
		String status_path = status_file_path + File.separator + table;
		
		File status_path_dir = new File(status_path);
		if(!status_path_dir.exists())
			status_path_dir.mkdirs();
		
		context.put(SqlSourceUtil.STATUS_DIRECTORY_KEY, status_path);
		context.put(SqlSourceUtil.TABLE_KEY, table);
		/* Initialize configuration parameters */
		jdbcContext = new HibernateContext(context, LOG.getName());

		/* Establish connection with database */
		hibernateReader = new HibernateReader(jdbcContext);
		hibernateReader.establishSession();

		System.out.println(jdbcContext.buildQuery());
		
		
		/* Instantiate the CSV Writer */
		csvWriter = new CSVWriter(new ChannelWriter(processor), ',');

	}

	public void close() {
		try {
			hibernateReader.closeSession();
			csvWriter.close();
		} catch (IOException e) {
			LOG.warn("Error CSVWriter object ", e);
		} finally {
		}
	}

	private class ChannelWriter extends Writer {
		private List<Event> events = new ArrayList<>();
		private ChannelProcessor processor;

		public ChannelWriter(ChannelProcessor processor) {
			this.processor = processor;
		}

		@Override
		public void write(char[] cbuf, int off, int len) throws IOException {
			Event event = new SimpleEvent();

			String s = new String(cbuf);
			event.setBody(s.substring(off, len - 1).getBytes());

			Map<String, String> headers;
			headers = new HashMap<String, String>();
			headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
			event.setHeaders(headers);

			events.add(event);

		}

		@Override
		public void flush() throws IOException {

			if(events!=null && !events.isEmpty()) {			
				LOG.info("flush event: " + events);
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
