package com.pplive.pike;

import java.util.Calendar;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.JSQLParserException;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

import com.pplive.pike.base.Period;
import com.pplive.pike.metadata.ITableInfoProvider;
import com.pplive.pike.metadata.TableManager;
import com.pplive.pike.metadata.TextFileTableInfoProvider;
import com.pplive.pike.parser.LogicalQueryPlan;
import com.pplive.pike.parser.ParseErrorsException;
import com.pplive.pike.parser.PikeSqlParser;
import com.pplive.pike.parser.SemanticErrorException;
import com.pplive.pike.generator.ISpoutGenerator;
import com.pplive.pike.generator.KafkaSpoutGenerator;
import com.pplive.pike.generator.LocalTextFileSpoutGenerator;
import com.pplive.pike.generator.trident.TridentTopologyGenerator;

public class PikeSqlCompiler {

	public static boolean parseSQLSyntax(String sql) {
		if (sql == null)
			throw new IllegalArgumentException("sql cannot be null");

		try {
			PikeSqlParser.parseSQLSyntax(sql);
			System.out.println("sql syntax ok");
			return true;
		} catch (JSQLParserException e) {
			System.err.println("sql syntax  error:");
			System.err.println(e.getCause() != null ? e.getCause() : e);
			return false;
		}
	}

	public static Map<String, Object> parseSQLOptions(String sql) {
		if (sql == null)
			throw new IllegalArgumentException("sql cannot be null");

		try {
			Map<String, Object> result = PikeSqlParser.parseSQLOptions(sql);
			return result;
		} catch (JSQLParserException e) {
			System.out.println("sql syntax  error:");
			System.out.println(e.getCause() != null ? e.getCause() : e);
			return null;
		} catch (SemanticErrorException e) {
			System.out.println("sql semantic  error:");
			System.out.println(e.getCause() != null ? e.getCause() : e);
			return null;
		} catch (ParseErrorsException e) {
			System.out.println("there are errors.");
			for (Exception err : e.getParseErrors()) {
				System.out.println(err.getCause() != null ? err.getCause()
						: err);
			}
			return null;
		}
	}

	public static LogicalQueryPlan parseSQL(String sql,
			TableManager tableManager, boolean optimize) {
		if (sql == null)
			throw new IllegalArgumentException("sql cannot be null");
		if (tableManager == null)
			throw new IllegalArgumentException("tableManager cannot be null");

		try {
			LogicalQueryPlan queryPlan = PikeSqlParser.parseSQL(sql,
					tableManager);
			if (optimize) {
				queryPlan.optimize();
			}
			return queryPlan;
		} catch (JSQLParserException e) {
			System.out.println("sql syntax  error:");
			System.out.println(e.getCause() != null ? e.getCause() : e);
			return null;
		} catch (SemanticErrorException e) {
			System.out.println("sql semantic  error:");
			System.out.println(e.getCause() != null ? e.getCause() : e);
			return null;
		} catch (UnsupportedOperationException e) {
			System.out.println("sql implementation problem:");
			System.out.println(e.getCause() != null ? e.getCause() : e);
			return null;
		} catch (ParseErrorsException e) {
			System.out.println("there are errors.");
			for (Exception err : e.getParseErrors()) {
				System.out.println(err.getCause() != null ? err.getCause()
						: err);
			}
			return null;
		}
	}

	private static TableManager createTableManager(AppOptions appOptions,
			Configuration conf) {
		if (appOptions.tableInfoFile().isEmpty()) {
			return new TableManager(conf);
		} else {
			TextFileTableInfoProvider tableInfoProvider = new TextFileTableInfoProvider(
					appOptions.tableInfoFile());
			return new TableManager(tableInfoProvider);
		}
	}

	private static ISpoutGenerator createSpoutGenerator(AppOptions appOptions,
			Configuration conf) {
		ITableInfoProvider tableInfoProvider = createTableManager(
				appOptions, conf).getProvider();
		if (appOptions.tableDataFile().isEmpty()) {
			return new KafkaSpoutGenerator(tableInfoProvider);
		} else {
			return new LocalTextFileSpoutGenerator(tableInfoProvider,
					appOptions.tableDataFile());
		}
	}

	public static PikeTopology compile(String topologyName, String sql,
			AppOptions appOptions) {
		Configuration conf = getTopologyConfig(topologyName, sql,
				appOptions.getOverridedTopologyConfig(), null);
		return compile(topologyName, sql, appOptions, conf);
	}

	public static PikeTopology compile(String topologyName, String sql,
			AppOptions appOptions, Configuration config) {
		if (topologyName == null)
			throw new IllegalArgumentException("topologyName cannot be null");
		if (sql == null)
			throw new IllegalArgumentException("sql cannot be null");
		if (appOptions == null)
			throw new IllegalArgumentException("appOptions cannot be null");

		Map<String, Object> options = parseSQLOptions(sql);
		if (options == null) {
			return null;
		}
		config.putAll(options);

		TableManager tableManager = createTableManager(appOptions, config);
		LogicalQueryPlan queryPlan = parseSQL(sql, tableManager, true);
		if (queryPlan == null)
			return null;

		ISpoutGenerator spoutGenerator = createSpoutGenerator(appOptions,
				config);
		boolean debug = appOptions.isDebugTopology();
		boolean localMode = appOptions.isLocalMode();
		if (appOptions.isDisplayTridentGraph()) {
			config.put("$$tridentgraph", Boolean.TRUE);
		}
		TridentTopologyGenerator generator = new TridentTopologyGenerator();
		StormTopology stormTopology = generator.generate(topologyName,
				queryPlan, spoutGenerator, config, debug, localMode);
		if (stormTopology == null) {
			return null;
		}
		return new PikeTopology(queryPlan, stormTopology, config);
	}

	private static Configuration getTopologyConfig(String topologyName,
			String sql, Properties overridedTopologyConfig,
			Map<String, Object> conf) {
		Configuration config = new Configuration();
		for (Map.Entry<Object, Object> kv : overridedTopologyConfig.entrySet()) {
			config.put(kv.getKey().toString(), tryParsePropertyValue(kv
					.getValue().toString()));
		}
		if (conf != null) {
			for (Map.Entry<String, Object> kv : conf.entrySet()) {
				config.put(kv.getKey(), kv.getValue());
			}
		}
		config.put(Configuration.TopologyJarVersion, BuildInfo.BuildVersion);
		config.put(Configuration.TopologyJarBuildTime, BuildInfo.BuildTime);
		config.put(Configuration.TopologyJarBuildMachine,
				BuildInfo.BuildMachine);
		config.put(Configuration.TopologyJarBuildBy, BuildInfo.BuildBy);
		config.put(Configuration.TopologySql, sql);
		return config;
	}

	private static Object tryParsePropertyValue(String val) {
		if (val.length() >= 2 && val.charAt(0) == '\''
				&& val.charAt(val.length() - 1) == '\'') {
			return val.substring(1, val.length() - 1);
		}
		if (val.equalsIgnoreCase("true"))
			return Boolean.TRUE;
		if (val.equalsIgnoreCase("false"))
			return Boolean.FALSE;
		try {
			return Integer.valueOf(val);
		} catch (NumberFormatException e) {
		}
		try {
			return Float.valueOf(val);
		} catch (NumberFormatException e) {
		}
		return val;
	}

	public static void EnsureTopologyMessageTimeout(Configuration conf,
			Period topologyPeriod) {
		int extraTimeoutSeconds = Configuration.getInt(conf,
				Configuration.MessageTimeoutSecondsAfterPeriod, 30);
		if (extraTimeoutSeconds < 1) {
			extraTimeoutSeconds = 1;
		}
		final int minTimeoutSeconds = topologyPeriod.periodSeconds()
				+ extraTimeoutSeconds;

		final Integer n = Configuration.getInt(conf,
				Configuration.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 1);
		if (n < minTimeoutSeconds) {
			conf.setMessageTimeoutSecs(minTimeoutSeconds);
		}
	}

	public static void compileAndSubmit(String topologyName, String sql,
			AppOptions appOptions) {
		compileAndSubmit(topologyName, sql, appOptions, null);
	}

	public static PikeTopology compileAndSubmit(String topologyName,
			String sql, AppOptions appOptions, Map<String, Object> conf) {
		Configuration config = getTopologyConfig(topologyName, sql,
				appOptions.getOverridedTopologyConfig(), conf);
		PikeTopology pikeTopology = compile(topologyName, sql, appOptions,
				config);
		if (pikeTopology == null)
			return pikeTopology;

		EnsureTopologyMessageTimeout(config, pikeTopology.period());
		if (config.isValidForStorm() == false) {
			throw new RuntimeException(
					"error: at least one option set in SQL or command line is not valid. Must be json-serializable");
		}

		submit(topologyName, config, pikeTopology.topology());
		return  pikeTopology;
	}

	public static void submit(String topologyName, Configuration config, StormTopology topology) {
		try {
			StormSubmitter.submitTopology(topologyName, config, topology);
			Logger log = LoggerFactory.getLogger(PikeSqlCompiler.class);
			log.info(String.format(
					"success: storm topology '%s' is submitted.", topologyName));
		} catch (AlreadyAliveException e) {
			throw new RuntimeException(String.format(
					"error: storm topology '%s' already exist and is alive.",
					topologyName), e);
		} catch (InvalidTopologyException e) {
			throw new RuntimeException(
					"should never happen: storm topology build result is invalid",
					e);
		}
	}

	public static void compileAndRunLocally(String topologyName, String sql,
			AppOptions appOptions) {
		Configuration config = getTopologyConfig(topologyName, sql,
				appOptions.getOverridedTopologyConfig(), null);
		PikeTopology pikeTopology = compile(topologyName, sql, appOptions,
				config);
		if (pikeTopology == null)
			return;

		EnsureTopologyMessageTimeout(config, pikeTopology.period());
		if (config.isValidForStorm() == false) {
			throw new RuntimeException(
					"error: at least one option set in SQL or command line is not valid. Must be json-serializable");
		}

		final int seconds = appOptions.localModeRunSeconds();
		runLocally(topologyName, seconds, config, pikeTopology.topology());

	}

	public static void runLocally(String topologyName, int runSenconds,  Configuration config, StormTopology topology) {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, config, topology);
		Calendar begin = Calendar.getInstance();
		try {
			while (true) {
				if (isExpired(begin, runSenconds)) {
					break;
				}
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {
			cluster.shutdown();
		}
	}

	private static boolean isExpired(Calendar begin, int seconds) {
		if (seconds <= 0)
			return false;

		Calendar t = Calendar.getInstance();
		t.add(Calendar.SECOND, -seconds);
		return t.after(begin);
	}
}
