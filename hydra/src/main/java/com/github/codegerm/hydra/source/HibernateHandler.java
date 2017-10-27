package com.github.codegerm.hydra.source;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.event.SqlEventBuilder;
import com.github.codegerm.hydra.reader.HibernateContext;
import com.github.codegerm.hydra.reader.HibernateReader;
import com.github.codegerm.hydra.writer.CsvWriter;


public class HibernateHandler extends AbstractHandler {

	
	protected HibernateContext jdbcContext;
	private CsvWriter csvWriter;
	private HibernateReader hibernateReader;
	private static final String DEFAULT_STATUS_DIRECTORY = "flume/jdbcSource/status";
	private static final Logger LOG = LoggerFactory.getLogger(HibernateHandler.class);
	private String status_file_path;
	


	public HibernateHandler(String snapshotId, Context context, ChannelProcessor processor, String table, String entitySchema) {
		super(snapshotId, context, processor, table, entitySchema);
	}

	@Override
	public void configure() {
		LOG.getName();

		LOG.info("Reading and processing configuration values for source " + LOG.getName());
		status_file_path = context.getString(SqlSourceUtil.STATUS_DIRECTORY_KEY, DEFAULT_STATUS_DIRECTORY);
		String status_path = status_file_path + File.separator + snapshotId + File.separator + table;

		File status_path_dir = new File(status_path);
		if (!status_path_dir.exists())
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
		csvWriter = new CsvWriter(processor, ',');

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



	@Override
	public Boolean handle() {
		try {
			List<List<Object>> result = hibernateReader.executeQuery();
			LOG.debug(result.toString());
			if (!result.isEmpty()) {
				csvWriter.writeAll(jdbcContext.getAllRows(result), true);
				csvWriter.flush();
				jdbcContext.updateStatusFile();
			}

		} catch (IOException | InterruptedException e) {
			LOG.error("Error procesing row", e);
			return false;
		} finally {
			close();
		}
		return true;
	}

}
