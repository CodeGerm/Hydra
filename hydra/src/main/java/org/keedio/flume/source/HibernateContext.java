package org.keedio.flume.source;

import org.keedio.flume.source.SQLSourceHelper;
import org.apache.flume.Context;

import com.github.codegerm.hydra.source.SqlSourceUtil;

public class HibernateContext extends SQLSourceHelper {
	
	private int pagedRows;
	private String delimiterEntry;
    private static final int DEFAULT_MAX_ROWS = 60000;
    private static final String DEFAULT_DELIMITER_ENTRY = ",";
    private String contextTable;
	public HibernateContext(Context context, String sourceName) {

		super(context, sourceName);
		pagedRows = context.getInteger("max.rows",DEFAULT_MAX_ROWS);
		delimiterEntry = context.getString("delimiter.entry",DEFAULT_DELIMITER_ENTRY);
		contextTable = context.getString(SqlSourceUtil.TABLE_KEY);
	}


	public int getPagedRows() {
		
		return pagedRows;
	}


	public String getDelimiterEntry() {
		return delimiterEntry;
	}
	
	public String getCountQuery(){
		
		String query  = "select count(*) from " + contextTable;
		return query;
	}
	
    public String getTableName(){
		return contextTable;
	}
	

}