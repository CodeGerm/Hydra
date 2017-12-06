package com.github.codegerm.hydra.trigger.record;

import org.apache.flume.Context;
import org.hibernate.SQLQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Only support if the table record is increased only
 */
public class NewRecordMonitor extends RecordMonitor {

	private static final Logger logger = LoggerFactory.getLogger(NewRecordMonitor.class);

	public NewRecordMonitor(Context context, String tableName, String primaryKeyName, RecordStatus lastStatus) {
		super(context, tableName, primaryKeyName, lastStatus);
	}

	@Override
	public RecordStatus checkRecord() {
		// Check for row count
		SQLQuery query = session.createSQLQuery(buildCountSQL());
		int count = ((Number) query.uniqueResult()).intValue();
		if (count > lastStatus.getRecordCount()) {
			logger.info("New record detected: [previousCount=" + lastStatus.getRecordCount() + ", newCount= " + count
					+ "]");

			// Get primary key value
			if (!Strings.isNullOrEmpty(primaryKeyName)) {
				query = session.createSQLQuery(buildPKSQL());
				query.setFirstResult(count - 1);
				query.setMaxResults(1);
				Object pkValue = query.uniqueResult();
				String pkString = pkValue == null ? "" : pkValue.toString();
				lastStatus.setPrimaryKeyValue(pkString);
			}

			lastStatus.setTriggered(true);
			lastStatus.setRecordCount(count);

			return lastStatus;
		} else if (count < lastStatus.getRecordCount()) {
			logger.warn("Detected record decreased, this trigger may not support it.");

			lastStatus.setTriggered(false);
			lastStatus.setRecordCount(count);
			lastStatus.setPrimaryKeyValue(null);
		} else {
			lastStatus.setTriggered(false);
		}
		return lastStatus;
	}

	private String buildCountSQL() {
		return "select count(*) from " + tableName;
	}

	private String buildPKSQL() {
		return "select " + primaryKeyName + " from " + tableName;
	}
}
