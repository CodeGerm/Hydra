package com.github.codegerm.hydra.trigger.record;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.hibernate.SQLQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlResultMonitor extends RecordMonitor {

	private static final Logger logger = LoggerFactory.getLogger(SqlResultMonitor.class);

	private String sql;
	private boolean firstTimeTrigger;

	public SqlResultMonitor(Context context, String sql, RecordStatus lastStatus, boolean firstTimeTrigger) {
		super(context, lastStatus);
		this.sql = sql;
		this.firstTimeTrigger = firstTimeTrigger;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RecordStatus checkRecord() {
		StringBuilder valueBuilder = new StringBuilder();
		try {
			SQLQuery query = session.createSQLQuery(sql);
			List<Object> result = query.list();
			for (Object row : result) {
				if (row instanceof Object[]) {
					for (Object cell : (Object[]) row) {
						valueBuilder.append(convertValue(cell));
					}
				} else {
					valueBuilder.append(convertValue(row));
				}

			}
		} catch (Exception e) {
			logger.warn("Execute SQL failed", e);
			return lastStatus;
		}

		String newValue = valueBuilder.toString();
		if (!StringUtils.equals(lastStatus.getRecordValue(), newValue)) {
			logger.info("Detected SQL result changed: from=" + lastStatus.getRecordValue() + ", to=" + newValue);			
			if (lastStatus.getRecordValue() == null && !firstTimeTrigger) {
				logger.info("First time record result, ignored");
				lastStatus.setTriggered(false);
			} else {								
				lastStatus.setTriggered(true);
			}
			lastStatus.setRecordValue(newValue);
		} else {
			lastStatus.setTriggered(false);
		}
		return lastStatus;
	}

	private String convertValue(Object value) {
		return value.toString();
	}

}
