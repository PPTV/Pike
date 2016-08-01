package com.pplive.pike.exec.output;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;


import com.pplive.pike.base.Period;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.pplive.pike.Configuration;
import com.pplive.pike.base.ISizeAwareIterable;

class JdbcOutput implements IPikeOutput {
	
	private BasicDataSource _dataSource;
	private String _dbTable;
	private int _batchInsertMaxCount;
	
	private JdbcWriter _writer;
	
	private static Logger logger() {
		return LoggerFactory.getLogger(JdbcOutput.class);
	}
	
	public JdbcOutput() {
	}
	
	@Override
	public void init(@SuppressWarnings("rawtypes") Map conf, OutputSchema outputSchema,String targetName, Period outputPeriod) {
		this._dbTable = targetName;
		
		String driverClass = (String)conf.get(Configuration.OutputJdbcDriver); // com.mysql.jdbc.Driver
		String dbUser = (String)conf.get(Configuration.OutputJdbcDbUser);
		String dbPassword = (String)conf.get(Configuration.OutputJdbcDbPassword);
		String dbUrl = (String)conf.get(Configuration.OutputJdbcDbUrl);
		if (StringUtils.isEmpty(driverClass) || StringUtils.isEmpty(dbUser) || dbPassword == null || StringUtils.isEmpty(dbUrl)){
			logger().error("JDBC Output configuration incomplete, please check. (driverClass, dbUser, dbPassword, dbUrl)");
			return;
		}
		this._dataSource = setupDataSource(driverClass, dbUser, dbPassword, dbUrl);
		this._batchInsertMaxCount = Configuration.getInt(conf, Configuration.OutputJdbcBatchInsertMaxCount, 100);
		if (this._batchInsertMaxCount <= 0) {
			throw new RuntimeException(String.format("config %s must be positive integer", Configuration.OutputJdbcBatchInsertMaxCount));
		}
		this._writer = new JdbcWriter();
		this._writer.init(this._dbTable, outputSchema, this._batchInsertMaxCount);
	}
	
	@Override
	public void write(Calendar periodEnd, ISizeAwareIterable<List<Object>> tuples) {
		if (tuples == null || tuples.size() == 0){
			return;
		}
		if (this._dataSource == null)
			return;
		Connection conn = null;
		try {
			conn = this._dataSource.getConnection();
			this._writer.insertRecords(conn, tuples);
		}
		catch(SQLException e){
			logger().error("JDBC Output get db connection error", e);
		}
		finally{
			if (conn != null){
				try{
					conn.close();
				}
				catch(SQLException e){
					logger().error("JDBC Output close db connection error", e);
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		closeDataSource(this._dataSource);
	}
	
	private static BasicDataSource setupDataSource(String driverClass, String dbUser, String dbPassword, String connectionUrl) {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(driverClass);
        ds.setUsername(dbUser);
        ds.setPassword(dbPassword);
        ds.setUrl(connectionUrl);
        ds.setPoolPreparedStatements(true);
        ds.setValidationQuery("select 1");
        return ds;
	}
	
	private static void closeDataSource(BasicDataSource ds){
		if (ds != null){
			try{
				ds.close();
			}
			catch(SQLException e){
				logger().error("JDBC Output close data source error", e);
			}
		}
	}
}
