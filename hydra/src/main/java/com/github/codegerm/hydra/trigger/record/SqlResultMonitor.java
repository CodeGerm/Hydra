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

	public SqlResultMonitor(Context context, String sql, RecordStatus lastStatus) {
		super(context, lastStatus);
		this.sql = sql;
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
						valueBuilder.append(cell);
					}
				} else {
					valueBuilder.append(row);
				}

			}
		} catch (Exception e) {
			logger.warn("Execute SQL failed", e);
			return lastStatus;
		}

		String newValue = valueBuilder.toString();
		if (!StringUtils.equals(lastStatus.getRecordValue(), newValue)) {
			lastStatus.setRecordValue(newValue);
			lastStatus.setTriggered(true);
		} else {
			lastStatus.setTriggered(false);
		}
		return lastStatus;
	}

}
