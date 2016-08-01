package com.pplive.pike;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONValue;

import backtype.storm.generated.TopologySummary;

import com.pplive.pike.automation.Pike;
import com.pplive.pike.base.PikeTopologyClient;
import com.pplive.pike.exec.output.OutputField;
import com.pplive.pike.exec.output.OutputSchema;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.metadata.Column;
import com.pplive.pike.metadata.Table;
import com.pplive.pike.metadata.TableManager;
import com.pplive.pike.metadata.TextFileTableInfoProvider;
import com.pplive.pike.parser.LogicalQueryPlan;
import com.pplive.pike.parser.LogicalQueryPlan.StreamingColumns;

public class Driver {
	public static final int OKResult = 0;
	public static final int UnknowError = -1;
	public static final int SQLError = -2;
	public static final int HelpResult = -3;
	public static final int ConfigError = -4;

	public static final Log log = LogFactory.getLog(Driver.class);

	public Table parseSchema(String sql) {
		return null;
	}

	public int execute(String sql) {
		return 0;
	}

	public int executeFile(File file) {
		return 0;
	}

	private static String generateTopologyName() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
		return String.format("topology_%s", dateFormat.format(new Date()));
	}

	public int execute(AppOptions appOptions) {
		assert appOptions != null;

		String sql;
		String topologyName = appOptions.topologyName();
		if (topologyName.isEmpty()) {
			topologyName = generateTopologyName();
		}

		if (appOptions.sqlFile().isEmpty() == false) {
			String fileName = appOptions.sqlFile();
			if (!fileName.startsWith("./")) {
				fileName = "./" + fileName;
			}
			sql = readSqlFromFile(appOptions.sqlFile());
		} else {
			assert appOptions.commandlineSQL().isEmpty() == false;
			sql = appOptions.commandlineSQL();
		}

		if (appOptions.isParseMode()) {
			boolean result = PikeSqlCompiler.parseSQLSyntax(sql);

			if (result) {
				System.out.println("SQL text parsed ok, no syntax error.");
			}
			return result ? OKResult : SQLError;
		}

		TableManager tableManager = createTableManager(appOptions);

		if (appOptions.isCompileMode() || appOptions.isExplainMode()) {
			boolean optimize = (appOptions.disableOptimize() == false);
			LogicalQueryPlan queryPlan = PikeSqlCompiler.parseSQL(sql,
					tableManager, optimize);
			if (queryPlan != null) {
				System.out
						.println("SQL text compiled success, no syntax/semantic error.");
				if (appOptions.isExplainMode())
					System.out.println(queryPlan.toExplainString());
				return OKResult;
			} else {
				return SQLError;
			}
		}

		if (appOptions.isGenerateMode()) {
			PikeTopology topology = PikeSqlCompiler.compile(topologyName, sql,
					appOptions);
			if (topology != null) {
				TopologyConsoleDisplayer.display(topology);
				System.out
						.println("topology is generated (not submitted), no error.");
				return OKResult;
			} else {
				return SQLError;
			}
		}

		if (appOptions.isLocalMode() == false) {
			if (appOptions.isConsolePrint()) {
				System.out.println("console print.");
				ConsoleSubmiterServer server = new ConsoleSubmiterServer();
				PikeTopology pikeTopology = PikeSqlCompiler.compileAndSubmit(
						topologyName, sql, appOptions, server.getThriftConf());
				StreamingColumns columns = pikeTopology.getQueryPlan()
						.getStreamingTableRequiredColumns();

				server.registColumns(pikeTopology.getConf(), columns.table()
						.getName(),
						new HashSet<String>(columns.requiredColumns()));

				System.out.println("compileAndSubmit success.");
				server.waitForResult();
			} else {

				PikeSqlCompiler.compileAndSubmit(topologyName, sql, appOptions);
			}

		} else {
			PikeSqlCompiler.compileAndRunLocally(topologyName, sql, appOptions);
		}
		return OKResult;
	}

	private static TableManager createTableManager(AppOptions appOptions) {
		if (appOptions.tableInfoFile().isEmpty()) {
			return new TableManager();
		} else {
			TextFileTableInfoProvider tableInfoProvider = new TextFileTableInfoProvider(
					appOptions.tableInfoFile());
			return new TableManager(tableInfoProvider);
		}
	}

	public int showTables(AppOptions appOptions) {
		assert appOptions != null;

		TableManager tableManager = createTableManager(appOptions);
		String[] tables = tableManager.getTableNames();
		for (String tableName : tables) {
			System.out.println(tableName);
		}
		return OKResult;
	}

	public int transferColumns(AppOptions appOptions) {
		try {
			String sql;
			if (appOptions.sqlFile().isEmpty() == false) {
				String fileName = appOptions.sqlFile();
				if (!fileName.startsWith("./")) {
					fileName = "./" + fileName;
				}
				sql = readSqlFromFile(appOptions.sqlFile());
			} else {
				assert appOptions.commandlineSQL().isEmpty() == false;
				sql = appOptions.commandlineSQL();
			}

			LogicalQueryPlan plan = Pike.parseSQL(sql, null);
			System.out.println(String.format("%s\t%s", plan
					.getStreamingTableRequiredColumns().table().getName(),
					StringUtils.join(plan.getStreamingTableRequiredColumns()
							.requiredColumns(), ",")));

			return OKResult;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return UnknowError;
		}

	}

	public int showSchema(AppOptions appOptions) {
		try {
			String sql;
			if (appOptions.sqlFile().isEmpty() == false) {
				String fileName = appOptions.sqlFile();
				if (!fileName.startsWith("./")) {
					fileName = "./" + fileName;
				}
				sql = readSqlFromFile(appOptions.sqlFile());
			} else {
				assert appOptions.commandlineSQL().isEmpty() == false;
				sql = appOptions.commandlineSQL();
			}

			OutputSchema outputSchema = Pike.getOutputSchema(sql);
			for (OutputField f : outputSchema.getOutputFields()) {
				System.out.println(String.format("%s\t%s", f.getName(),
						f.getValueType()));
			}
			return OKResult;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return UnknowError;
		}

	}

	public int descTable(AppOptions appOptions) {
		assert appOptions != null;

		TableManager tableManager = createTableManager(appOptions);
		String tableName = appOptions.tableName();
		Column[] columns = tableManager.getTable(tableName).getColumns();
		if (columns == null) {
			System.out.println(String
					.format("table '%s' not found.", tableName));
			return -1;
		}

		System.out.println("Name\tType\tTitle");
		for (Column column : columns) {
			System.out.println(String.format("%s\t%s\t%s", column.getName(),
					this.columnTypeToString(column), column.getTitle()));
		}
		return OKResult;
	}

	private String columnTypeToString(Column column) {
		if (column.getColumnType() == ColumnType.Map_ObjString) {
			return "map<string,string>";
		} else if (column.getColumnType() == ColumnType.Complex) {
			return column.getColumnTypeValue();
		} else
			return column.getColumnType().toString();
	}

	private static Pattern _passwordPattern = Pattern.compile(
			"password|passwd", Pattern.CASE_INSENSITIVE);

	private static boolean isIngoredConfigItem(String itemName) {
		if (itemName.equals(Configuration.TopologySql))
			return true;
		Matcher matcher = _passwordPattern.matcher(itemName);
		if (matcher.find()) {
			return true;
		}
		return false;
	}

	public int showTopologyConfig(AppOptions appOptions) {
		final String topologyName = appOptions.topologyName();
		@SuppressWarnings("rawtypes")
		Map topologyConf = getTopologyConf(topologyName);
		if (topologyConf == null) {
			System.out.println(String.format(
					"there is no running topology with name %s", topologyName));
			return -1;
		}

		ArrayList<String> keys = new ArrayList<String>(topologyConf.size());
		for (Object o : topologyConf.keySet()) {
			keys.add(o.toString());
		}
		Collections.sort(keys);
		for (String k : keys) {
			if (isIngoredConfigItem(k)) {
				System.out.println(String.format("%s : <...>", k));
				continue;
			}
			Object o = topologyConf.get(k);
			System.out.println(String.format("%s : %s", k, JSONValue
					.toJSONString(o).replace("\\/", "/")));
		}

		return 0;
	}

	public int showTopologies(AppOptions appOptions) {
		Configuration conf = Configuration.getStormConf();
		PikeTopologyClient client = PikeTopologyClient.getConfigured(conf);
		List<TopologySummary> topologies = client.getRunningTopologies();
		System.out
				.println("Name  Status  UpTimeSeconds  Workers  Executors  Tasks  Id");
		for (TopologySummary topo : topologies) {
			System.out.println(String.format("%s\t\t%s\t%d\t%d\t%d\t%d\t%s",
					topo.get_name(), topo.get_status(), topo.get_uptime_secs(),
					topo.get_num_workers(), topo.get_num_executors(),
					topo.get_num_tasks(), topo.get_id()));
		}
		return 0;
	}

	public int showTopologySql(AppOptions appOptions) {
		final String topologyName = appOptions.topologyName();
		@SuppressWarnings("rawtypes")
		Map topologyConf = getTopologyConf(topologyName);
		if (topologyConf == null) {
			System.out.println(String.format(
					"there is no running topology with name %s", topologyName));
			return -1;
		}
		String sql = Configuration.getString(topologyConf,
				Configuration.TopologySql);
		if (sql.isEmpty()) {
			System.out.println(String
					.format("topology %s has no sql, not a pike topology",
							topologyName));
			return -1;
		}
		System.out.println(sql);
		return 0;
	}

	@SuppressWarnings("rawtypes")
	public static Map getTopologyConf(String topologyName) {
		Configuration conf = Configuration.getStormConf();
		PikeTopologyClient client = PikeTopologyClient.getConfigured(conf);
		String id = client.getRunningTopologyIdByName(topologyName);
		if (id.isEmpty()) {
			return null;
		}
		Map topologyConf = client.getTopologyConf(id);
		return topologyConf;
	}

	public int addDbTable(AppOptions appOptions) {
		final String sql = appOptions.commandlineSQL();
		assert sql.isEmpty() == false;

		BasicDataSource ds = setupDataSource();
		Connection conn = null;
		Statement statement = null;
		try {
			conn = ds.getConnection();
			statement = conn.createStatement();
			statement.executeUpdate(sql);
			System.out.println("done, SQL is successfully executed.");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (statement != null) {
				try {
					statement.close();
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
			closeDataSource(ds);
		}
		return 0;
	}

	private static BasicDataSource setupDataSource() {
		Configuration conf = new Configuration();

		String driverClass = Configuration.getString(conf,
				Configuration.OutputJdbcDriver, "");
		String dbUser = Configuration.getString(conf,
				Configuration.OutputJdbcDbUser, "");
		String dbPassword = Configuration.getString(conf,
				Configuration.OutputJdbcDbPassword, "");
		String dbUrl = Configuration.getString(conf,
				Configuration.OutputJdbcDbUrl, "");
		if (driverClass.isEmpty() || dbUser.isEmpty() || dbUrl.isEmpty()) {
			System.err
					.println("JDBC Output configuration incomplete, please check. (driverClass, dbUser, dbPassword, dbUrl)");
			return null;
		}

		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(driverClass);
		ds.setUsername(dbUser);
		ds.setPassword(dbPassword);
		ds.setUrl(dbUrl);
		ds.setValidationQuery("select 1");
		return ds;
	}

	private static void closeDataSource(BasicDataSource ds) {
		if (ds != null) {
			try {
				ds.close();
			} catch (SQLException e) {
			}
		}
	}

	private static String readSqlFromFile(String path) {
		if (path == null)
			throw new IllegalArgumentException("path cannot be null");

		BufferedReader reader = null;
		File file = new File(path);
		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(file), "utf-8"));
			StringBuffer sb = new StringBuffer();
			String line;
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (line.length() == 0 || line.startsWith(";")
						|| line.startsWith("//"))
					continue;
				sb.append(line + " ");
			}
			return sb.toString();
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(
					"should never happen: this JRE/OS does not support UTF8 encoding",
					e);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(String.format("file not found: %s",
					file.getAbsolutePath()), e);
		} catch (IOException e) {
			throw new RuntimeException(String.format("file IO error: %s",
					file.getAbsolutePath()), e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
