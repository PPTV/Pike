package com.pplive.pike.function.builtin;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.AbstractUDF;
import com.pplive.pike.base.AbstractUdtf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Join {
	public static final Logger log = LoggerFactory.getLogger(Join.class);

	public static class MemoryJoin extends AbstractUDF {
		/**
		 * 
		 */
		private static final long serialVersionUID = -960770160489456138L;

		public static String evaluate(Object value, String tableName,
				String joinColumnName, String outputColumn) {
			MemoryJoinProvider provider = getProivder(true, tableName,
					joinColumnName, outputColumn);
			return provider.toOne(value)[0];

		}

		public static String evaluate(Object value, String tableName,
				String outputColumn) {
			return evaluate(value, tableName, "SequenceId", outputColumn);
		}

	}

	public static class MemoryTransJoin extends AbstractUdtf {
		/**
		 * 
		 */
		private static final long serialVersionUID = -960770160489456138L;

		public static String[][] evaluate(Object value, boolean toOne,
				String tableName, String joinColumnName,
				String... outputColumns) {
			MemoryJoinProvider provider = getProivder(toOne, tableName,
					joinColumnName, outputColumns);
			return provider.toN(value);

		}

		public static String[][] evaluate(Object value, boolean toOne,
				String tableName, String outputColumn) {
			return evaluate(value, false, tableName, "SequenceId", outputColumn);
		}

		public static String[] evaluate(Object value, String tableName,
				String joinColumnName, String... outputColumns) {
			MemoryJoinProvider provider = getProivder(true, tableName,
					joinColumnName, outputColumns);
			return provider.toOne(value);

		}

		public static String[] evaluate(Object value, String tableName,
				String outputColumn) {
			return evaluate(value, tableName, "SequenceId", outputColumn);
		}

	}

	static {
		Timer timer = new Timer();
		timer.schedule(new Task(), new Date(), 3600000);
	}

	private static class Task extends TimerTask {

		@Override
		public void run() {
			synchronized (lock) {
				memoryJoinMap = new HashMap<String, Join.MemoryJoinProvider>();
			}
		}

	}

	private static Map<String, MemoryJoinProvider> memoryJoinMap = new HashMap<String, MemoryJoinProvider>();
	private static Object lock = new Object();

	private static MemoryJoinProvider getProivder(boolean toOne,
			String tableName, String joinColumnName, String... outputColumns) {
		String key = formatKey(toOne, tableName, joinColumnName, outputColumns);
		MemoryJoinProvider provider = memoryJoinMap.get(key);
		if (provider == null) {
			synchronized (lock) {
				provider = memoryJoinMap.get(key);
				if (provider == null) {
					provider = new MemoryJoinProvider(toOne, tableName,
							joinColumnName, outputColumns);
					memoryJoinMap.put(key, provider);
				}
			}
		}
		return provider;
	}

	private static String formatKey(boolean toOne, String tableName,
			String joinColumnName, String... outputColumns) {
		return String.format("%s-%s-%s-%s", toOne ? "one" : "n", tableName,
				joinColumnName, StringUtils.join(outputColumns, ","));
	}

	private static class MemoryJoinProvider {
		private Map<String, String> toOneMap;
		private Map<String, List<String[]>> toNMap;
		private Map<String, String[]> toOneArrayMap;

		private Map<Integer, Integer> channelIntMap;
		private Map<Integer, Integer[]> channelIntArrayMap;
		private Map<Integer, String> channelStringMap;
		private Map<Integer, String[]> channelStringArrayMap;

		private final String ChannelTableName = "dim_sync_channel";
		private final String[] ChannelIntColumnNames = new String[] {
				"SequenceId", "ext_Id", "ext_parentId", "AlternateKey",
				"RedirectTo", "SeriesId", "Dim_LiveOndemand",
				"Dim_ContentType", "Dim_SubCategoryId", "Dim_Copyright",
				"Dim_VideoLength", "SeriesChannelId", "BaikeId", "Status",
				"CategoryId", "BitrateKbps", "PlaybackDurationSeconds",
				"ResolutionHeight", "ResolutionWidth", "isVIP",
				"SubSeriesCount", "ContentProvider" };
		private final boolean toOne;
		private final int outputColumnLength;
		private final boolean isKeyInt;
		private final boolean isValueInt;

		public MemoryJoinProvider(boolean toOne, String tableName,
				String joinColumnName, String... outputColumns) {
			this.toOne = toOne;
			this.outputColumnLength = outputColumns.length;
			this.isKeyInt = this.toOne
					&& ChannelTableName.equalsIgnoreCase(tableName);
			this.isValueInt = this.isKeyInt && this.isIntColumn(outputColumns);
			this.fillData(this.buildSql(tableName, joinColumnName,
					outputColumns));
		}

		private boolean isIntColumn(String[] columnNames) {
			for (int i = 0; i < columnNames.length; i++) {
				boolean isInt = false;
				for (int j = 0; j < ChannelIntColumnNames.length; j++) {
					if (ChannelIntColumnNames[j]
							.equalsIgnoreCase(columnNames[i])) {
						isInt = true;
						break;
					}

				}
				if (!isInt)
					return false;
			}
			return true;
		}

		private String buildSql(String tableName, String joinColumnName,
				String... outputColumns) {
			if (tableName.indexOf('.') < 0) {
				tableName = "dw_common." + tableName;
			}

			return String.format("select %s,%s from %s", joinColumnName,
					StringUtils.join(outputColumns, ","), tableName);
		}

		private void fillData(String sql) {
			Connection connection = null;
			try {
				connection = this.setupDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement(sql);
				ResultSet resultSet = statement.executeQuery();
				while (resultSet.next()) {
					String[] values = new String[this.outputColumnLength];
					Integer[] intValues = new Integer[this.outputColumnLength];

					String key = null;
					Integer intKey = null;

					if (this.isKeyInt) {
						intKey = resultSet.getInt(1);
					} else {
						Object v = resultSet.getObject(1);
						key = v == null ? null : v.toString();
					}
					if (this.toOne) {
						if (this.isValueInt) {
							for (int i = 0; i < this.outputColumnLength; i++) {
								intValues[i] = resultSet.getInt(i + 2);
							}

							if (this.outputColumnLength == 1) {
								if (isKeyInt) {
									if (this.channelIntMap == null)
										this.channelIntMap = new HashMap<Integer, Integer>();
									this.channelIntMap
											.put(intKey, intValues[0]);
								}
							} else {
								if (this.channelIntArrayMap == null)
									this.channelIntArrayMap = new HashMap<Integer, Integer[]>();
								this.channelIntArrayMap.put(intKey, intValues);
							}
						} else {
							for (int i = 0; i < this.outputColumnLength; i++) {
								Object v = resultSet.getObject(i + 2);
								values[i] = v == null ? null : v.toString();
							}
							if (this.outputColumnLength == 1) {
								if (isKeyInt) {
									if (this.channelStringMap == null)
										this.channelStringMap = new HashMap<Integer, String>();
									this.channelStringMap
											.put(intKey, values[0]);
								} else {
									if (this.toOneMap == null)
										this.toOneMap = new HashMap<String, String>();
									this.toOneMap.put(key, values[0]);
								}
							} else {
								if (isKeyInt) {
									if (this.channelStringArrayMap == null)
										this.channelStringArrayMap = new HashMap<Integer, String[]>();
									this.channelStringArrayMap.put(intKey,
											values);
								} else {
									if (this.toOneArrayMap == null)
										this.toOneArrayMap = new HashMap<String, String[]>();
									this.toOneArrayMap.put(key, values);
								}
							}

						}
					} else {
						if (this.toNMap == null)
							this.toNMap = new HashMap<String, List<String[]>>();
						List<String[]> valueList = this.toNMap.get(key);
						if (valueList == null) {
							valueList = new ArrayList<String[]>();
							this.toNMap.put(key, valueList);
						}
						for (int i = 0; i < this.outputColumnLength; i++) {
							Object v = resultSet.getObject(i + 2);
							values[i] = v == null ? null : v.toString();
						}
						valueList.add(values);

					}

				}
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

		public String[] toOne(Object value) {
			if (value == null) {
				return new String[this.outputColumnLength];
			}
			if (this.isValueInt) {
				if (this.outputColumnLength == 1) {
					Integer intValue = this.channelIntMap.get(Integer
							.parseInt(value.toString()));
					return new String[] { intValue == null ? null : intValue
							.toString() };

				} else {
					Integer[] intValues = this.channelIntArrayMap.get(Integer
							.parseInt(value.toString()));
					if (intValues != null) {
						String[] values = new String[intValues.length];
						for (int i = 0; i < intValues.length; i++) {
							values[i] = intValues[i] == null ? null
									: intValues[i].toString();
						}
						return values;
					} else
						return new String[this.outputColumnLength];
				}
			} else {

				if (this.outputColumnLength == 1) {
					if (isKeyInt) {
						return new String[] { this.channelStringMap.get(Integer
								.parseInt(value.toString())) };

					} else {

						return new String[] { this.toOneMap.get(value
								.toString()) };
					}
				} else {
					String[] values;
					if (isKeyInt) {

						values = this.channelStringArrayMap.get(Integer
								.parseInt(value.toString()));
					} else {
						values = this.toOneArrayMap.get(value.toString());
					}
					return values == null ? new String[this.outputColumnLength]
							: values;
				}

			}
		}

		public String[][] toN(Object value) {
			if (value == null) {
				return new String[][] { new String[this.outputColumnLength] };
			}
			List<String[]> valueList = this.toNMap.get(value.toString());
			if (valueList != null) {
				String[][] result = new String[valueList.size()][];
				valueList.toArray(result);
				return result;
			} else {
				return new String[][] { new String[this.outputColumnLength] };
			}

		}

		private BasicDataSource setupDataSource() {
			Configuration conf = new Configuration();
			String driverClass = (String) conf
					.get(Configuration.SqlServerBulkClassName); // com.mysql.jdbc.Driver
			String dbUser = (String) conf.get(Configuration.SqlServerBulkUser);
			String dbPassword = (String) conf
					.get(Configuration.SqlServerBulkPassword);
			String dbUrl = (String) conf.get(Configuration.SqlServerBulkDBUrl);

			BasicDataSource ds = new BasicDataSource();
			ds.setDriverClassName(driverClass);
			ds.setUsername(dbUser);
			ds.setPassword(dbPassword);
			ds.setUrl(dbUrl);
			ds.setPoolPreparedStatements(true);
			// ds.setValidationQuery("select 1");
			return ds;
		}

	}

}
