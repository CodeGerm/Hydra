package com.github.codegerm.hydra;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import com.github.codegerm.hydra.event.EventBuilder;
import com.github.codegerm.hydra.event.SqlEventBuilder;
import com.github.codegerm.hydra.schema.ColumnSchema;
import com.github.codegerm.hydra.schema.ModelSchema;
import com.github.codegerm.hydra.schema.EntitySchema;
import com.github.codegerm.hydra.source.SqlEventDrivenSource;
import com.github.codegerm.hydra.source.SqlSource;
import com.github.codegerm.hydra.source.SqlSourceUtil;
import com.github.codegerm.hydra.task.Task;
import com.github.codegerm.hydra.task.TaskRegister;
import com.github.codegerm.hydra.writer.AvroRecordUtil;
import com.github.codegerm.hydra.writer.AvroWriter;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
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
	private static final String[] DB_TABLE_2_NAMES = { "Compaq", "Pan Am" };
	private static final String SOURCE_STATUS_DIR = "flume/test/status";
	private static final Gson gson = new Gson();
	private Channel channel;
	private Channel eventDrivenchannel;

	private SqlEventDrivenSource eventDrivenSource;
	private SqlSource source;
	private Map<String, String> entitySchemas;
	private Map<String, String> entitySchemas2;
	private Map<String, String> entitySchemas3;

	@Before
	public void setup() throws SQLException {
		DatabaseSetup();
		avroSchemaSetup();
		pollableflumeSetup();
		eventDrivenflumeSetup();
	}

	private Connection getDBConnection() {
		Connection dbConnection = null;
		try {
			Class.forName(DB_DRIVER);
		} catch (ClassNotFoundException e) {
			LOG.error("db driver not found: ", e);
		}
		try {
			dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
			return dbConnection;
		} catch (SQLException e) {
			LOG.error("db connection error: ", e);
		}
		return dbConnection;
	}

	public void DatabaseSetup() throws SQLException {

		DeleteDbFiles.execute("db", "testdb", false);

		tableSetup(DB_TABLE_1, DB_TABLE_1_NAMES);
		tableSetup(DB_TABLE_2, DB_TABLE_2_NAMES);
	}

	@Deprecated
	public void schemaSetup() {
		List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
		columns.add(new ColumnSchema("id", Integer.class.getName()));
		columns.add(new ColumnSchema("name", String.class.getName()));
		columns.add(new ColumnSchema("date", Timestamp.class.getName()));
		columns.add(new ColumnSchema("isDeleted", Boolean.class.getName()));
		List<EntitySchema> tables = new ArrayList<EntitySchema>();
		tables.add(new EntitySchema("EMPLOYEE", columns));
		tables.add(new EntitySchema("COMPANY", columns));
		ModelSchema model = new ModelSchema("testModel", tables);
		System.out.println(gson.toJson(model));
	}

	public void avroSchemaSetup() {
		String table1 = "Employee";
		String schema1 = "{\"name\":\"Employee\",\"type\":\"record\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"date\",\"type\":\"long\"},{\"name\":\"isDeleted\",\"type\":\"boolean\"}]}";

		String table2 = "COMPANY";
		String schema2 = "{\"name\":\"COMPANY\",\"type\":\"record\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"isDeleted\",\"type\":\"boolean\"}]}";

		entitySchemas = new HashMap<String, String>();
		entitySchemas.put(table1, schema1);
		entitySchemas.put(table2, schema2);

		entitySchemas2 = new HashMap<String, String>();
		entitySchemas2.put(table1, schema1);

		entitySchemas3 = new HashMap<String, String>();
		entitySchemas3.put(table2, schema2);
	}

	public void tableSetup(String tableName, String[] Values) throws SQLException {
		Connection connection = getDBConnection();
		PreparedStatement createPreparedStatement = null;
		PreparedStatement insertPreparedStatement = null;
		PreparedStatement selectPreparedStatement = null;

		String CreateQuery = "CREATE TABLE " + tableName
				+ "(id int primary key, name varchar(255), date timestamp, isDeleted boolean)";
		String InsertQuery = "INSERT INTO " + tableName + "(id, name, date, isDeleted) values" + "(?,?,?,?)";
		String SelectQuery = "select * from " + tableName;

		try {
			connection.setAutoCommit(false);

			createPreparedStatement = connection.prepareStatement(CreateQuery);
			createPreparedStatement.executeUpdate();
			createPreparedStatement.close();
			for (int i = 0; i < Values.length; i++) {
				insertPreparedStatement = connection.prepareStatement(InsertQuery);
				insertPreparedStatement.setInt(1, i + 1);
				insertPreparedStatement.setString(2, Values[i]);
				Timestamp ts = new Timestamp(System.currentTimeMillis());
				insertPreparedStatement.setTimestamp(3, ts);
				insertPreparedStatement.setBoolean(4, false);
				insertPreparedStatement.executeUpdate();
				insertPreparedStatement.close();
			}

			selectPreparedStatement = connection.prepareStatement(SelectQuery);
			ResultSet rs = selectPreparedStatement.executeQuery();
			while (rs.next()) {
				LOG.info("row: [Id " + rs.getInt("id") + " Name " + rs.getString("name") + "] inserted to [" + tableName
						+ "] table");
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

	public void pollableflumeSetup() {
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
		context.put("hibernate.connection.driver_class", DB_DRIVER);
		context.put("status.file.name", "statusFile");
		context.put("status.file.path", SOURCE_STATUS_DIR + "/pollable");
		context.put(SqlSourceUtil.POLL_INTERVAL_KEY, "1000");
		context.put(SqlSourceUtil.TIMEOUT_KEY, "1000");
		context.put(SqlSourceUtil.MODEL_ID_KEY, "testModel");
		context.put(SqlSourceUtil.MODE_KEY, "TASK");
		context.put(SqlSourceUtil.MODEL_SCHEMA_KEY, gson.toJson(entitySchemas));
		// source.setEntitySchemas(entitySchemas);
		source.configure(context);

	}

	public void eventDrivenflumeSetup() {
		eventDrivenSource = new SqlEventDrivenSource();
		eventDrivenchannel = new MemoryChannel();

		Configurables.configure(eventDrivenchannel, new Context());

		List<Channel> channels = new ArrayList<>();
		channels.add(eventDrivenchannel);

		ChannelSelector rcs = new ReplicatingChannelSelector();
		rcs.setChannels(channels);

		eventDrivenSource.setChannelProcessor(new ChannelProcessor(rcs));
		Context context = new Context();
		context.put("hibernate.connection.url", DB_CONNECTION);
		context.put("hibernate.connection.user", DB_USER);
		context.put("hibernate.connection.password", DB_PASSWORD);
		context.put("hibernate.connection.driver_class", DB_DRIVER);
		context.put("status.file.name", "statusFile");
		context.put("status.file.path", SOURCE_STATUS_DIR + "/eventdriven");
		context.put(SqlSourceUtil.POLL_INTERVAL_KEY, "1000");
		context.put(SqlSourceUtil.TIMEOUT_KEY, "1000");
		eventDrivenSource.configure(context);

	}

	// @Test
	public void runTaskMode() {
		System.out.println("Testing pollable sql source in task mode: ");
		source.start();
		Task task = new Task(entitySchemas, "testTaskMode");
		TaskRegister.getInstance().addTask(task);
		try {
			source.process();
		} catch (EventDeliveryException e2) {
			e2.printStackTrace();
		}
		validateResult(channel);

	}

	// @Test
	public void runScheduleMode() {
		source.start();
		try {
			source.process();
		} catch (EventDeliveryException e2) {
			e2.printStackTrace();
		}
		validateResult(channel);

	}

	@Test
	public void runEventDrivenSource() {
		System.out.println("Testing event driven sql source: ");
		eventDrivenSource.start();
		Task task = new Task(entitySchemas3, "testTaskMode");
		TaskRegister.getInstance().addTask(task);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		task = new Task(entitySchemas2, "testTaskMode");
		TaskRegister.getInstance().addTask(task);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		source.stop();
		validateResult(eventDrivenchannel);

	}

	private void validateResult(Channel channel) {
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
			if (e.getHeaders().get(EventBuilder.EVENT_TYPE_KEY) != null
					&& e.getHeaders().get(EventBuilder.EVENT_TYPE_KEY).equals(SqlEventBuilder.EVENT_TYPE)) {
				if (e.getHeaders().get(EventBuilder.WRITER_TYPE_KEY) == null) {
					System.out.println("No writer type defined");
				} else if (e.getHeaders().get(EventBuilder.WRITER_TYPE_KEY).equals(AvroWriter.WRITER_TYPE)) {
					String entity = e.getHeaders().get(EventBuilder.ENTITY_NAME_KEY);
					String schema = entitySchemas.get(entity);
					if (schema == null) {
						System.out.println("No schema found");
						continue;
					}
					try {
						String str = AvroRecordUtil.deserialize(e.getBody(), schema).toString();
						System.out.println("Event header: " + e.getHeaders() + ", Event body: " + str);
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				} else {
					String str = new String(e.getBody(), Charsets.UTF_8);
					System.out.println("Event header: " + e.getHeaders() + ", Event body: " + str);
				}
			} else {
				String str = new String(e.getBody(), Charsets.UTF_8);
				System.out.println("Event header: " + e.getHeaders() + ", Event body: " + str);
			}

		}
	}

	@After
	public void cleanup() {
		try {
			DeleteDbFiles.execute("db", "testdb", false);
			File sourceDir = new File(SOURCE_STATUS_DIR);
			FileUtils.deleteDirectory(sourceDir);
		} catch (Exception e) {
			LOG.error("delete temp testing files failed: ", e);
		}

	}

}
