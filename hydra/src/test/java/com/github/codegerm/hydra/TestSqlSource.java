package com.github.codegerm.hydra;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.h2.tools.DeleteDbFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.source.SqlSource;
import com.github.codegerm.hydra.source.SqlSourceUtil;
import com.google.common.base.Charsets;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSqlSource {
	private static final Logger LOG = LoggerFactory.getLogger(TestSqlSource.class);

	private static final String DB_DRIVER = "org.h2.Driver";
	private static final String DB_CONNECTION = "jdbc:h2:./db/testdb";
	private static final String DB_USER = "";
	private static final String DB_PASSWORD = "";
	private static final String DB_TABLE_1 = "EMPLOYEE";
	private static final String[] DB_TABLE_1_NAMES = { "Hanks", "Jones" };
	private static final String DB_TABLE_2 = "COMPANY";
	private static final String SOURCE_STATUS_DIR = "flume/test/status";
	private Channel channel;
	private SqlSource source;

	@Before
	public void setup() throws SQLException {
	    DatabaseSetup();
		flumeSetup();
	}

	private Connection getDBConnection() {
		Connection dbConnection = null;
		try {
			Class.forName(DB_DRIVER);
		} catch (ClassNotFoundException e) {
			LOG.error("db create error: ", e);
		}
		try {
			dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
			return dbConnection;
		} catch (SQLException e) {
			LOG.error("db create error: ", e);
		}
		return dbConnection;
	}

	public void DatabaseSetup() throws SQLException {

		DeleteDbFiles.execute("db", "testdb", false);

		Connection connection = getDBConnection();
		PreparedStatement createPreparedStatement = null;
		PreparedStatement insertPreparedStatement = null;
		PreparedStatement selectPreparedStatement = null;

		String CreateQuery = "CREATE TABLE " + DB_TABLE_1 + "(id int primary key, name varchar(255))";
		String InsertQuery = "INSERT INTO " + DB_TABLE_1 + "(id, name) values" + "(?,?)";
		String SelectQuery = "select * from " + DB_TABLE_1;

		try {
			connection.setAutoCommit(false);

			createPreparedStatement = connection.prepareStatement(CreateQuery);
			createPreparedStatement.executeUpdate();
			createPreparedStatement.close();
			for (int i = 0; i < DB_TABLE_1_NAMES.length; i++) {
				insertPreparedStatement = connection.prepareStatement(InsertQuery);
				insertPreparedStatement.setInt(1, i + 1);
				insertPreparedStatement.setString(2, DB_TABLE_1_NAMES[i]);
				insertPreparedStatement.executeUpdate();
				insertPreparedStatement.close();
			}

			selectPreparedStatement = connection.prepareStatement(SelectQuery);
			ResultSet rs = selectPreparedStatement.executeQuery();
			while (rs.next()) {
				LOG.info("row: [Id " + rs.getInt("id") + " Name " + rs.getString("name") + "] inserted");
			}
			selectPreparedStatement.close();

			connection.commit();
		} catch (SQLException e) {
			System.out.println("Exception Message " + e.getLocalizedMessage());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			connection.close();
		}
	}

	public void flumeSetup() {
		source = new SqlSource();
		channel = new MemoryChannel();

		Configurables.configure(channel, new Context());

		List<Channel> channels = new ArrayList<>();
		channels.add(channel);

		ChannelSelector rcs = new ReplicatingChannelSelector();
		rcs.setChannels(channels);

		source.setChannelProcessor(new ChannelProcessor(rcs));
		Context context = new Context();
		context.put("hibernate.connection.url", DB_CONNECTION);
		context.put("hibernate.connection.user", DB_USER);
		context.put("hibernate.connection.password", DB_PASSWORD);
		context.put("table", "public." + DB_TABLE_1);
		context.put("hibernate.connection.driver_class", DB_DRIVER);
		context.put("status.file.name", "statusFile");
		context.put("status.file.path", SOURCE_STATUS_DIR);
		context.put(SqlSourceUtil.POLL_INTERVAL_KEY, "1000");

		source.configure(context);

	}

	@Test
	public void runTest() {
		source.start();

		try {
			source.process();
		} catch (EventDeliveryException e2) {
			e2.printStackTrace();
		}
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		List<Event> channelEvents = new ArrayList<>();
		Transaction txn = channel.getTransaction();
		txn.begin();
		for (int i = 0; i < 10; i++) {
			Event e = channel.take();
			if (e == null) {
				break;
				// throw new NullPointerException("Event is null");
			}
			channelEvents.add(e);
		}

		try {
			txn.commit();
		} catch (Throwable t) {
			txn.rollback();
		} finally {
			txn.close();
			source.stop();
		}

		for (Event e : channelEvents) {
			String str = new String(e.getBody(), Charsets.UTF_8);
			System.out.println(str);
			System.out.println(e.getHeaders());
		}

	}

	@After
	public void cleanup() {
		try {
			DeleteDbFiles.execute("db", "testdb", false);
			File sourceDir = new File(SOURCE_STATUS_DIR);
			FileUtils.deleteDirectory(sourceDir);
		} catch (Exception e) {
			LOG.error("delete temp file failed: ", e);
		}

	}

}
