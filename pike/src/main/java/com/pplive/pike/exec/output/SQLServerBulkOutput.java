package com.pplive.pike.exec.output;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tools.ant.util.DateUtils;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.util.Path;

public class SQLServerBulkOutput implements IPikeOutput {
	public static final Logger log = LoggerFactory.getLogger(SQLServerBulkOutput.class);

	private BasicDataSource _dataSource;
	private String tableName;
	private String schemaName;
	private Integer[] indexes;
	private String[] columnNames;
	private String winBulkPath;
	private String linuxDBFilePath;
	private ColumnType[] columnTypes;
	private final SimpleDateFormat format = new SimpleDateFormat(
			"yyyyMMddHHmmss");
	private final DecimalFormat df = new DecimalFormat("##.##");

	@Override
	public void init(@SuppressWarnings("rawtypes") Map conf,
			OutputSchema outputSchema, String targetName, Period outputPeriod) {
		int dotIndex = targetName.indexOf('.');
		if (dotIndex < 0) {
			this.tableName = targetName;
			this.schemaName = "diy"; // todo, PPTV specific, put in configuration
		} else {
			this.schemaName = targetName.substring(0, dotIndex);
			this.tableName = targetName.substring(dotIndex + 1);
		}
		Object winBulkPath = conf.get(Configuration.SqlServerBulkWinDBFile);
		Object linuxDBFilePath = conf
				.get(Configuration.SqlServerBulkLinuxDBFile);
		this.winBulkPath = (String) winBulkPath;
		this.linuxDBFilePath = (String) linuxDBFilePath;
		String driverClass = (String) conf
				.get(Configuration.SqlServerBulkClassName); // com.mysql.jdbc.Driver
		String dbUser = (String) conf.get(Configuration.SqlServerBulkUser);
		String dbPassword = (String) conf
				.get(Configuration.SqlServerBulkPassword);
		String dbUrl = (String) conf.get(Configuration.SqlServerBulkDBUrl);
		this._dataSource = setupDataSource(driverClass, dbUser, dbPassword,
				dbUrl);
		List<String> columns = this.getColumns(this.tableName);
		List<Integer> indexes = new ArrayList<Integer>();
		List<String> columnNames = new ArrayList<String>();
		List<ColumnType> columnTypes = new ArrayList<ColumnType>();
		for (String column : columns) {
			int index = outputSchema.indexOfColumn(column);
			indexes.add(index);
			columnNames.add(column);
			if (index >= 0) {
				columnTypes.add(outputSchema.getOutputField(index)
						.getValueType());
			} else {
				columnTypes.add(null);
			}
		}
		this.indexes = new Integer[indexes.size()];
		this.columnNames = new String[indexes.size()];
		this.columnTypes = new ColumnType[indexes.size()];
		indexes.toArray(this.indexes);
		columnNames.toArray(this.columnNames);
		columnTypes.toArray(this.columnTypes);

	}

	private List<String> getColumns(String tableName) {
		List<String> columns = new ArrayList<String>();
		Connection connection = null;
		try {
			connection = this._dataSource.getConnection();
			PreparedStatement statement = connection
					.prepareStatement(String
							.format("select syscolumns.name as columnname,systypes.name as columntype "
									+ "from sysobjects inner join syscolumns on syscolumns.id = sysobjects.id inner "
									+ "join systypes on syscolumns.xusertype= systypes.xusertype "
									+ "where sysobjects.name ='%s' order by syscolumns.colorder",
									tableName));
			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String name = resultSet.getString(1);
				columns.add(name);
			}
			return columns;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e1) {
					log.error("sql server connection close exception.", e1);
				}
			}
		}
	}

	private static BasicDataSource setupDataSource(String driverClass,
			String dbUser, String dbPassword, String connectionUrl) {
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(driverClass);
		ds.setUsername(dbUser);
		ds.setPassword(dbPassword);
		ds.setUrl(connectionUrl);
		ds.setPoolPreparedStatements(true);
		// ds.setValidationQuery("select 1");
		return ds;
	}

	private String getFilePath(String rootPath, Long dateTime) {
		String parentPath = Path.combine(rootPath, Path.combine(
				String.format("%s.%s", this.schemaName, this.tableName),
				String.format("dt=%d", dateTime)));
		File file = new File(parentPath);
		if (!file.exists())
			file.mkdirs();
		String filePath = Path.combine(parentPath, "data.csv");
		return filePath;
	}

	@Override
	public void write(Calendar periodEnd,
			ISizeAwareIterable<List<Object>> tuples) {
		Long dateTimeValue = Long
				.parseLong(format.format(periodEnd.getTime()));
		int length = this.indexes.length;
		String filePath = this.getFilePath(this.linuxDBFilePath, dateTimeValue);

		File file = new File(filePath);
		if (!file.getParentFile().exists()) {
			if (!file.getParentFile().mkdirs()) {
				throw new RuntimeException("directory " + file.getParent()
						+ " create error.");
			}
		}
		log.info("write path:" + filePath);
		OutputStreamWriter streamWriter = null;
		BufferedWriter writer = null;
		try {
			streamWriter = new OutputStreamWriter(
					new FileOutputStream(filePath), "gbk");
			writer = new BufferedWriter(streamWriter);
			writer.write(StringUtils.join(this.columnNames, '\t'));
			writer.write("\r\n");
			for (List<Object> vs : tuples) {
				for (int i = 0; i < length; i++) {
					if (i > 0)
						writer.write("\t");
					int index = this.indexes[i];
					Object v = vs.get(index);
					if (v != null) {
						writer.write(this.valueToString(v, this.columnTypes[i]));
					}
				}
				writer.write("\r\n");
			}

			writer.flush();
			streamWriter.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					log.error("writer close error.", e);
				}
			}
			if (streamWriter != null) {
				try {
					streamWriter.close();
				} catch (IOException e) {
					log.error("writer close error.", e);
				}
			}
		}
		this.importFileToSqlServer(dateTimeValue);
		if (!file.delete() || !file.getParentFile().delete()) {
			log.error("delete file error.");
		}

	}

	private void importFileToSqlServer(Long dateTime) {
		String errorRootPath = Path.combine(Path.combine(this.winBulkPath,
				"error"), String.format("%s.%s_data_%d.csv", this.schemaName,
				this.tableName, dateTime));
		String dataFileRootPath = this.getFilePath(this.winBulkPath, dateTime);
		Connection connection = null;
		try {
			String sql = String
					.format("BULK INSERT %s.%s FROM '%s' with(  DATAFILETYPE = 'char',FIELDTERMINATOR = '%s', ROWTERMINATOR = '\\n',"
							+ " MAXERRORS = %d,ERRORFILE = '%s',FIRSTROW=%d)",
							this.schemaName, tableName, dataFileRootPath,
							"\\t", 0, errorRootPath, 2);
			log.info("sql exec:" + sql);
			connection = this._dataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.execute();
		} catch (SQLException e) {
			throw new RuntimeException("import error table name "
					+ this.tableName, e);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e1) {
					log.error("sql server connection close exception.", e1);
				}
			}
		}

	}

	private String valueToString(Object value, ColumnType columnType) {
		switch (columnType) {
		case Boolean:
			return value.equals(true) ? "1" : "0";

		case Double:
			return df.format((Double) value);
		case Float:
			return df.format((Float) value);
		case Byte:
		case Int:
		case Long:
		case Short:
			return value.toString();
		case Date:
			return DateUtils.format((Date) value, "yyyy-MM-dd");

		case Map_ObjString:
		case String:
		case Unknown:
			return value.toString();
		case Time:
			return DateUtils.format((Date) value, "yyyy-MM-dd HH:mm:ss");
		case Timestamp:
			return "";
		default:
			return value.toString();
		}
	}

	@Override
	public void close() throws IOException {

	}

}
