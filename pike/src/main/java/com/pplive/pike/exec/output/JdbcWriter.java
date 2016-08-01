package com.pplive.pike.exec.output;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.exec.spoutproto.ColumnType;

class JdbcWriter {

	private static Logger logger() {
		//return LogFactory.getLog(JdbcWriter.class);
		return LoggerFactory.getLogger("LocalConsoleOutput");
	}
	
	private static class InsertSql {
		public final int insertCount;
		public final String sql;
		public InsertSql(int n, String s){
			this.insertCount = n;
			this.sql = s;
		}
	}
	
	private OutputSchema _outputSchema;
	private ArrayList<InsertSql> _sqls;
	
	public void init(String table, OutputSchema outputSchema, int batchInsertMaxCount) {
		assert table != null && table.isEmpty() == false;
		assert outputSchema != null;

		this._outputSchema = outputSchema;
		this._sqls = new ArrayList<InsertSql>(16);
		int n = batchInsertMaxCount;
		while (n > 0) {
			String sql = createSql(table, outputSchema, n);
			this._sqls.add(new InsertSql(n, sql));
			n /= 2;
		}
	}
	
	public void insertRecords(Connection connection, ISizeAwareIterable<List<Object>> tuples) {
		assert connection != null;
		assert tuples != null && tuples.size() > 0;
		
		int tupleLeftCount = tuples.size();
		Iterator<List<Object>> iter = tuples.iterator();

		logger().debug(String.format("JdbcWriter.insertRecords: %d tuples", tuples.size()));
		while (tupleLeftCount > 0) {
			InsertSql insertSql = findSql(tupleLeftCount); 
			ArrayList<List<Object>> insertTuples = copyPart(iter, insertSql.insertCount);
			tupleLeftCount -= insertSql.insertCount;
			
			assert insertSql != null;
			assert insertTuples.size() > 0;
			
			PreparedStatement statement = null;
			try {
				statement = connection.prepareStatement(insertSql.sql);
				fillStatement(statement, this._outputSchema, insertTuples);
				logger().debug("JdbcWriter.insertRecord(), executeUpdate ...");
				statement.executeUpdate();
			} catch (SQLException e) {
				logger().error(String.format("JDBC Writer execute SQL error: %s%nsql:%n%s", e.getMessage(), insertSql.sql), e);
			} finally {
				if (statement != null) {
					try {
						statement.close();
					} catch (SQLException e) {
						logger().error(String.format("JDBC Writer close prepared statement error"), e);
					}
				}
			}
		}
	}
	
	private InsertSql findSql(int tupleLeftCount) {
		assert tupleLeftCount > 0;
		for(InsertSql is : this._sqls){
			if (tupleLeftCount >= is.insertCount){
				return is;
			}
		}
		assert false;
		return null;
	}
	
	private static ArrayList<List<Object>> copyPart(Iterator<List<Object>> iter, int count) {
		assert count > 0;
		ArrayList<List<Object>> items = new ArrayList<List<Object>>(count);
		int n = 0;
		while(n < count && iter.hasNext()){
			items.add(iter.next());
			n += 1;
		}
		return items;
	}

	private static void fillStatement(PreparedStatement statement, OutputSchema outputSchema,
									ArrayList<List<Object>> tuples) throws SQLException {
		int n = 0;
		for(List<Object> data : tuples) {
			int fieldCount = -1;
			for (OutputField f : outputSchema.getOutputFields()) {
				n += 1;
				fieldCount += 1;
				int sqlType = convertToSqlType(f.getValueType());
				assert fieldCount < data.size();
				Object o = data.get(fieldCount);
				if (o == null) {
					statement.setNull(n, sqlType);
				} 
				else {
					if (sqlType == java.sql.Types.VARCHAR) {
						o = o.toString();
					}
					statement.setObject(n, o, sqlType);
				}
			}
			assert fieldCount + 1 == data.size();
		}
		assert n == statement.getParameterMetaData().getParameterCount();
	}

	private static int convertToSqlType(ColumnType columnType) {
		switch (columnType) {
		case Unknown:
			return java.sql.Types.VARCHAR;
		case Boolean:
			return java.sql.Types.BIT;
		case String:
			return java.sql.Types.VARCHAR;
		case Byte:
			return java.sql.Types.TINYINT;
		case Short:
			return java.sql.Types.SMALLINT;
		case Int:
			return java.sql.Types.INTEGER;
		case Long:
			return java.sql.Types.BIGINT;
		case Float:
			return java.sql.Types.REAL;
		case Double:
			return java.sql.Types.DOUBLE;
		case Map_ObjString:
			return java.sql.Types.VARCHAR;
		case Date:
			return java.sql.Types.DATE;
		case Time:
			return java.sql.Types.TIME;
		case Timestamp:
			return java.sql.Types.TIMESTAMP;
		default:
			return java.sql.Types.VARCHAR;
		}
	}

	private static String createSql(String table, OutputSchema outputSchema, int insertCount) {
		assert insertCount > 0;
		
		String s = "insert into @{table} (@{columns}) values (@{values})";

		s = s.replaceAll("@\\{table\\}", table);
		s = s.replaceAll("@\\{columns\\}", createColumns(outputSchema.getOutputFields()));
		s = s.replaceAll("@\\{values\\}", createValues(outputSchema.getOutputFields()));
		if (insertCount == 1) {
			return s;
		}
		
		StringBuilder sql = new StringBuilder(s.length() + (insertCount-1) * 100);
		sql.append(s);
		for (int n = 1; n < insertCount; n +=1) {
			sql.append(",(").append(createValues(outputSchema.getOutputFields())).append(')');
		}
		return sql.toString();
	}

	private static String createColumns(Iterable<OutputField> outputFields) {
		int n = -1;
		StringBuilder sb = new StringBuilder(50);
		for (OutputField f : outputFields) {
			n += 1;
			if (n >= 1) {
				sb.append(", ");
			}
			sb.append('`').append(f.getName()).append('`');
		}
		return sb.toString();
	}

	private static String createValues(Iterable<OutputField> outputFields) {
		int n = -1;
		StringBuilder sb = new StringBuilder(100);
		for (@SuppressWarnings("unused") OutputField f : outputFields) {
			n += 1;
			if (n >= 1) {
				sb.append(", ?");
			} else {
				sb.append('?');
			}
		}
		return sb.toString();
	}
}
