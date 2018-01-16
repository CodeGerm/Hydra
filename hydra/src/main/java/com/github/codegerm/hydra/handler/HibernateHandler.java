package com.github.codegerm.hydra.handler;

import java.io.File;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.keedio.flume.source.HibernateContext;
import org.keedio.flume.source.HibernateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.source.SqlSourceUtil;
import com.github.codegerm.hydra.writer.AvroJsonWriter;
import com.github.codegerm.hydra.writer.AvroRecordUtil;
import com.github.codegerm.hydra.writer.RecordWriter;


public class HibernateHandler extends AbstractHandler {

	protected HibernateContext jdbcContext;

	private RecordWriter recordWriter;
	private HibernateReader hibernateReader;
	private static final String DEFAULT_STATUS_DIRECTORY = "flume/jdbcSource/status";
	private static final String COLUMN_TO_SELECT_KEY = "columns.to.select";
	private static final Logger LOG = LoggerFactory.getLogger(HibernateHandler.class);

	public HibernateHandler(String snapshotId, Context context, ChannelProcessor processor, String modelId, String table, String entitySchema) {
		super(snapshotId, context, processor, modelId, table, entitySchema);
	}

	@Override
	public void configure() {
		LOG.getName();

		LOG.info("Reading and processing configuration values for source " + LOG.getName());
		String basePath = context.getString(SqlSourceUtil.STATUS_BASE_DIR_KEY, DEFAULT_STATUS_DIRECTORY);
		String statusPath = basePath + File.separator + snapshotId + File.separator + table;

		File status_path_dir = new File(statusPath);
		if (!status_path_dir.exists())
			status_path_dir.mkdirs();

		context.put(SqlSourceUtil.STATUS_DIRECTORY_KEY, statusPath);
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
		recordWriter = new AvroJsonWriter(processor, snapshotId, modelId, entitySchema);
	}

	public void close() {
		try {
			hibernateReader.closeSession();
			recordWriter.close();
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
				recordWriter.writeAll(result);
				recordWriter.flush();
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
