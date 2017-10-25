package com.github.codegerm.hydra.reader;

import java.util.List;

import org.keedio.flume.source.HibernateHelper;
import org.keedio.flume.source.SQLSourceHelper;


public class HibernateReader extends HibernateHelper implements JDBCReader {

	public HibernateReader(SQLSourceHelper helper) {
		super(helper);
	}

	public List<List<Object>> executePagedQuery(int pageSize) throws InterruptedException {
		//TOD:implement
		
		return null;
	}

}