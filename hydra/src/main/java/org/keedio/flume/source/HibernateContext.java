package org.keedio.flume.source;

import org.keedio.flume.source.SQLSourceHelper;
import org.apache.flume.Context;

public class HibernateContext extends SQLSourceHelper {
	
	private int pagedRows;
	private String delimiterEntry;
    private static final int DEFAULT_MAX_ROWS = 10000;
    private static final String DEFAULT_DELIMITER_ENTRY = ",";
    
	public HibernateContext(Context context, String sourceName) {

		super(context, sourceName);
		pagedRows = context.getInteger("max.rows",DEFAULT_MAX_ROWS);
		delimiterEntry = context.getString("delimiter.entry",DEFAULT_DELIMITER_ENTRY);
	}


	public int getPagedRows() {
		
		return pagedRows;
	}


	public String getDelimiterEntry() {
		return delimiterEntry;
	}
}