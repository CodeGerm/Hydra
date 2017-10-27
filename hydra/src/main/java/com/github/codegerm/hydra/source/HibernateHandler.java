package com.github.codegerm.hydra.source;

import java.io.File;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.reader.HibernateContext;
import com.github.codegerm.hydra.reader.HibernateReader;
import com.github.codegerm.hydra.writer.AvroRecordUtil;
import com.github.codegerm.hydra.writer.AvroWriter;


public class HibernateHandler extends AbstractHandler {

	
	protected HibernateContext jdbcContext;
	//private CsvWriter csvWriter;
	private AvroWriter avroWriter;
	private HibernateReader hibernateReader;
	private static final String DEFAULT_STATUS_DIRECTORY = "flume/jdbcSource/status";
	private static final String COLUMN_TO_SELECT_KEY = "columns.to.select";
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
		
		List<String> columns = AvroRecordUtil.getEntityFields(entitySchema);
		String columnToSelect = StringUtils.join(columns, ",");
		context.put(COLUMN_TO_SELECT_KEY, columnToSelect);
		
		/* Initialize configuration parameters */
		jdbcContext = new HibernateContext(context, LOG.getName());

		/* Establish connection with database */
		hibernateReader = new HibernateReader(jdbcContext);
		hibernateReader.establishSession();

		System.out.println(jdbcContext.buildQuery());

		/* Instantiate the CSV Writer */
		//csvWriter = new CsvWriter(processor, ',', entitySchema);
		avroWriter = new AvroWriter(processor, entitySchema);
	}

	public void close() {
		try {
			hibernateReader.closeSession();
			avroWriter.close();
		} catch (Exception e) {
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
				avroWriter.writeAll(result);
				avroWriter.flush();
				jdbcContext.updateStatusFile();
			}

		} catch (Exception e) {
			LOG.error("Error procesing row", e);
			return false;
		} finally {
			close();
		}
		return true;
	}

}
