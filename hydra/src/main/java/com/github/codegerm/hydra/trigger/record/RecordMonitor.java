package com.github.codegerm.hydra.trigger.record;

import java.sql.Connection;
import java.util.Iterator;
import java.util.Map;

import org.apache.flume.Context;
import org.hibernate.CacheMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.service.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RecordMonitor {

	private static final Logger logger = LoggerFactory.getLogger(RecordMonitor.class);

	protected static SessionFactory factory;

	protected ServiceRegistry serviceRegistry;
	protected Configuration config;
	protected Session session;

	protected RecordStatus lastStatus;

	public RecordMonitor(Context context, RecordStatus lastStatus) {
		this.lastStatus = lastStatus;

		config = buildHibernateConfig(context);
		config.setProperty(Environment.USE_QUERY_CACHE, Boolean.FALSE.toString());
		config.setProperty(Environment.USE_SECOND_LEVEL_CACHE, Boolean.FALSE.toString());
		config.setProperty(Environment.ISOLATION, String.valueOf(Connection.TRANSACTION_READ_UNCOMMITTED));
	}

	public void setInitialStatus(RecordStatus status) {
		this.lastStatus = status;
	}

	public void establishSession() {
		logger.info("Opening hibernate session");
		serviceRegistry = new StandardServiceRegistryBuilder().applySettings(config.getProperties()).build();
		factory = config.buildSessionFactory(serviceRegistry);
		session = factory.openSession();
		session.setCacheMode(CacheMode.IGNORE);
		session.setDefaultReadOnly(true);
	}

	public void closeSession() {
		logger.info("Closing hibernate session");
		if (session != null) {
			session.close();
		}
	}

	protected abstract RecordStatus checkRecord();

	private Configuration buildHibernateConfig(Context context) {
		Map<String, String> hibernateProperties = context.getSubProperties("hibernate.");
		Iterator<Map.Entry<String, String>> it = hibernateProperties.entrySet().iterator();
		Configuration config = new Configuration();
		Map.Entry<String, String> e;
		while (it.hasNext()) {
			e = it.next();
			config.setProperty("hibernate." + e.getKey(), e.getValue());
		}
		return config;
	}

}
