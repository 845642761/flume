package org.me.flume.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * flume: mysqlSink
 * @author: chengbo
 * @date: 2016年1月13日 09:55:45
 */
public class MysqlSink extends AbstractSink implements Configurable {
	private Logger log = Logger.getLogger(MysqlSink.class);
	private String hostname;
	private String port;
	private String databaseName;
	private String tableName;
	private String logValue;
	private String user;
	private String password;
	private PreparedStatement preparedStatement;
	private Connection conn;
	private int batchSize;

	@Override
	public void configure(Context context) {
		hostname = context.getString("hostname");
		Preconditions.checkNotNull(hostname, "hostname must be set!!");
		port = context.getString("port");
		Preconditions.checkNotNull(port, "port must be set!!");
		databaseName = context.getString("databaseName");
		Preconditions.checkNotNull(databaseName, "databaseName must be set!!");
		tableName = context.getString("tableName");
		Preconditions.checkNotNull(tableName, "tableName must be set!!");
		logValue = context.getString("logValue");
		Preconditions.checkNotNull(logValue, "logValue must be set!!");
		user = context.getString("user");
		Preconditions.checkNotNull(user, "user must be set!!");
		password = context.getString("password");
		Preconditions.checkNotNull(password, "password must be set!!");
		batchSize = context.getInteger("batchSize", 100);
		Preconditions.checkNotNull(batchSize > 0,"batchSize must be a positive number!!");
	}

	@Override
	public void start() {
		super.start();
		try {
			log.debug("start loading jdbc driver...");
			// 调用Class.forName()方法加载驱动程序
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			log.error(e);
			e.printStackTrace();
		}

		String url = "jdbc:mysql://" + hostname + ":" + port + "/"
				+ databaseName;
		
		// 调用DriverManager对象的getConnection()方法，获得一个Connection对象
		try {
			conn = DriverManager.getConnection(url, user, password);
			conn.setAutoCommit(false);
			StringBuffer sb = new StringBuffer();
			sb.append("insert into ");
			sb.append(tableName);
			sb.append(" (");
			sb.append(logValue);
			sb.append(") values (?)");
			// 创建一个Statement对象
			preparedStatement = conn.prepareStatement(sb.toString());
		} catch (SQLException e) {
			log.error(e);
			e.printStackTrace();
			System.exit(1);
		}
		log.debug("loading jdbc driver completed!");
	}

	@Override
	public void stop() {
		super.stop();
		log.debug("start close the connection!");
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		log.debug("close the connection completed!");
	}

	@Override
	public Status process() throws EventDeliveryException {
		log.debug("start process event...");
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event;
		String content;
		List<String> actions = Lists.newArrayList();
		transaction.begin();
		try {
			for (int i = 0; i < batchSize; i++) {
				event = channel.take();
				if (event != null) {
					content = new String(event.getBody());
					actions.add(content);
				} else {
					result = Status.BACKOFF;
					break;
				}
			}
			if (actions.size() > 0) {
				preparedStatement.clearBatch();
				for (String temp : actions) {
					preparedStatement.setString(1, temp);
					preparedStatement.addBatch();
				}
				preparedStatement.executeBatch();
				conn.commit();
			}
			transaction.commit();
		} catch (Throwable e) {
			try {
				transaction.rollback();
			} catch (Exception e2) {
				log.error("Exception in rollback. Rollback might not have been" + "successful.", e2);
			}
			log.error("Failed to commit transaction." + "Transaction rolled back.", e);
			Throwables.propagate(e);
		} finally {
			transaction.close();
		}
		log.debug("process event completed!");
		return result;
	}
}
