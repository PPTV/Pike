package com.pplive.pike;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class Configuration extends Config {

	private static final Logger log = LoggerFactory.getLogger(Configuration.class);

	private static final long serialVersionUID = 2114720814246040005L;

	public static final String PIKE_CONF_DIR_KEY = "PIKE_CONF_DIR";
	private static final String STORM_HOME_DIR = "STORM_HOME";

	public static Configuration getStormConf() {
		String stormHome = System.getenv(STORM_HOME_DIR);
		if (stormHome == null || stormHome.isEmpty()) {
			String msg = String
					.format("environment var %s is undefined, cannot know where storm config file is",
							STORM_HOME_DIR);
			throw new RuntimeException(msg);
		}
		if (stormHome.endsWith(File.separator) == false) {
			stormHome += File.separator;
		}
		String filePath = stormHome + "conf/storm.yaml";

		File file = new File(filePath);
		if (file.exists() == false) {
			String msg = String
					.format("cannot find storm config file: %s (the directory is decided by environment var %s)",
							filePath, STORM_HOME_DIR);
			throw new RuntimeException(msg);
		}
		return new Configuration(file);
	}

	public Configuration() {
		String pikeConfDir = System.getenv(PIKE_CONF_DIR_KEY);
		if (pikeConfDir == null || pikeConfDir.isEmpty()) {
			log.warn("environment var {} is undefined, cannot know where pike config file is",
					PIKE_CONF_DIR_KEY);
			return;
		}
		if (pikeConfDir.endsWith(File.separator) == false) {
			pikeConfDir += File.separator;
		}
		String filePath = pikeConfDir + "pike.yaml";

		File file = new File(filePath);
		if (!file.exists()) {
			log.warn("cannot find pike config file: {} (the directory is decided by environment var {})",
					filePath, PIKE_CONF_DIR_KEY);
			return;
		}
		initialize(file);
	}

	public Configuration(File confFile) {
		if (!confFile.exists()) {
			String msg = String
					.format("cannot find pike config file: %s (the directory is decided by environment var %s)",
							confFile.getPath(), PIKE_CONF_DIR_KEY);
			throw new RuntimeException(msg);
		}
		initialize(confFile);
	}

	public void addResource(URL url) {
		initialize(new File(url.getPath()));
	}

	public boolean isValidForStorm() {
		return Utils.isValidConf(this);
	}

	@SuppressWarnings("unchecked")
	private void initialize(File confFile) {

		FileInputStream fileInputStream = null;
		InputStreamReader reader = null;
		try {
			Charset utf8 = Charset.forName("UTF-8");
			fileInputStream = new FileInputStream(confFile);
			reader = new InputStreamReader(fileInputStream, utf8);
			Yaml yaml = new Yaml();
			@SuppressWarnings("rawtypes")
			Map ret = (Map) yaml.load(reader);
			if (ret != null) {
				this.putAll(ret);
			}
			if (isValidForStorm() == false) {
				throw new RuntimeException(
						"Pike conf is not valid. Must be json-serializable");
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
			}
			try {
				if (fileInputStream != null)
					fileInputStream.close();
			} catch (IOException e) {
			}
		}
	}

	public Integer getInt(String key) {
		return getInt(this, key);
	}

	public Boolean getBoolean(String key) {
		return getBoolean(this, key);
	}

	public String getString(String key) {
		return getString(this, key);
	}

	public static Integer getInt(Map map, String key) {
		return getInt(map, key, null);
	}
	
	

	public static Integer getInt(Map map, String key, Integer defaultOnMissing) {
		Object o = map.get(key);
		if (o == null)
			return defaultOnMissing;
		if (o instanceof Integer) {
			return (Integer) o;
		}
		if (o instanceof Long) {
			return ((Long) o).intValue();
		}
		if (o instanceof Short) {
			return ((Short) o).intValue();
		}
		try {
			return Integer.parseInt(o.toString());
		} catch (NumberFormatException e) {
			return defaultOnMissing;
		}
	}

	public static Long getLong(Map map, String key) {
		return getLong(map, key, null);
	}

	public static Long getLong(Map map, String key, Long defaultOnMissing) {
		Object o = map.get(key);
		if (o == null)
			return defaultOnMissing;
		if (o instanceof Long) {
			return (Long) o;
		}
		if (o instanceof Integer) {
			return ((Integer) o).longValue();
		}
		if (o instanceof Short) {
			return ((Short) o).longValue();
		}
		try {
			return Long.parseLong(o.toString());
		} catch (NumberFormatException e) {
			return defaultOnMissing;
		}
	}

	public static Boolean getBoolean(Map map, String key) {
		return getBoolean(map, key, false);
	}
	
	public static Boolean getBoolean(Map map, String key,
			boolean defaultOnMissing) {
		Object o = map.get(key);
		if (o == null)
			return defaultOnMissing;
		if (o instanceof Boolean) {
			return (Boolean) o;
		}
		if (o instanceof Number) {
			return ((Number) o).intValue() != 0;
		}
		String s = o.toString();
		return (s.isEmpty() || s.equalsIgnoreCase("1") || s
				.equalsIgnoreCase("false")) == false;
	}

	public static String getString(Map map, String key) {
		return getString(map, key, "");
	}

	public static String getString(Map map, String key, String defaultOnMissing) {
		Object o = map.get(key);
		return o != null ? o.toString() : defaultOnMissing;
	}

	public boolean isDebugTopology() {
		return getBoolean(debugTopology);
	}

	public boolean isLocalMode() {
		return getBoolean(localMode);
	}

	public boolean isDisplayTridentGraph() {
		return getBoolean(displayTridentGraph);
	}

	public int localModeRunSeconds() {
		return getInt(localRunSeconds);
	}

	public boolean isOptimizeQL() { return getBoolean(optimizeQL); }

	public String getSchemaThriftURL() {
		return this.getString(SchemaThriftURL);
	}

	public static final String OUTPUT_CLASS_LIST = "pike.output.classes"; // list
																			// of
																			// string

	public static final String debugTopology = "pike.topology.debug";
	public static final String localMode = "pike.topology.local";
	public static final String localRunSeconds = "pike.topology.local.seconds";
	public static final String displayTridentGraph = "pike.topology.graph.display";
	public static final String optimizeQL = "pike.ql.optimize";

	public static final String TopologyJarVersion = "pike.topology.jar.build.version"; // string
	public static final String TopologyJarBuildTime = "pike.topology.jar.build.time"; // string
	public static final String TopologyJarBuildMachine = "pike.topology.jar.build.machine"; // string
	public static final String TopologyJarBuildBy = "pike.topology.jar.build.by"; // string
	public static final String TopologySql = "pike.topology.sql"; // string

	public final static String MessageTimeoutSecondsAfterPeriod = "pike.message.timeout_after_period.seconds"; // integer

	public final static String SchemaThriftURL = "pike.schema.thrift.url"; // string

	public final static String TridentParallelismHint = "pike.topogenerate.trident.parallelismhint"; // integer

	public static final String SpoutTaskCount = "pike.spout.tasks"; // integer
	public static final String SpoutKafkaZKServers = "pike.spout.kafka.zookeeper.servers"; //string
	public static final String SpoutKafkaZKSessionTimeout = "pike.spout.kafka.zookeeper.session.timeout.ms";
	public static final String SpoutRabbitmqPrefechCount = "pike.spout.rabbitmq.prefetch.count";
	public static final String SpoutRabbitmqHost = "pike.spout.rabbitmq.host"; // string
	public static final String SpoutRabbitmqPort = "pike.spout.rabbitmq.port"; // integer
	public static final String SpoutRabbitmqUsername = "pike.spout.rabbitmq.username"; // string
	public static final String SpoutRabbitmqPassword = "pike.spout.rabbitmq.password"; // string
	public static final String SpoutRabbitmqVirtualHost = "pike.spout.rabbitmq.vhost"; // string
	public static final String SpoutRabbitmqDataCharset = "pike.spout.rabbitmq.data.charset"; // string
	public static final String SpoutRabbitmqDataFieldSeparator = "pike.spout.rabbitmq.data.field_separator"; // string
	public static final String SpoutRabbitmqPrintQueueData = "pike.spout.rabbitmq.print_queuedata"; // boolean
	public static final String SpoutRabbitmqPrintEmitData = "pike.spout.rabbitmq.print_emitdata"; // boolean
	public static final String SpoutRabbitmqTimeoutMilliseconds = "pike.spout.rabbitmq.data.timeout.milliseconds"; // integer
	public static final String SpoutRabbitmqCheckTopologyselfKilled = "pike.spout.rabbitmq.check_topologyself_killed"; // boolean
    public static final String SpoutRabbitmqCheckKilledIntervalSeconds = "pike.spout.rabbitmq.check_killed.interval_seconds"; // integer
    public static final String SpoutRabbitmqExchangeName = "pike.spout.rabbitmq.exchange_name"; // string
    public static final String SpoutRabbitmqRoutingKey = "pike.spout.rabbitmq.routing_key"; // string
    public static final String SpoutRabbitmqReconnectWaitSeconds = "pike.spout.rabbitmq.reconnect_waitseconds"; // integer

    public static final String OutputCheckTopologyselfKilled = "pike.output.check_topologyself_killed"; // boolean
    public static final String OutputCheckMinPeriodSeconds = "pike.output.check_min_period_seconds"; // integer
	public static final String OutputFirstPeriodResult = "pike.output.first_period_output"; // boolean
	public static final String OutputLastPeriodResult = "pike.output.last_period_output"; // boolean
	public static final String OutputDefaultTargets = "pike.output.targets.default"; // list
																						// of
																						// string

	public static final String OutputRollingHeader = "pike.output.rolling.header"; // boolean
	public static final String OutputFileSuffix = "pike.output.file.suffix"; // string
	public static final String OutputFileSingle = "pike.output.file.single"; // boolean
	public static final String OutputFileFieldSeparator = "pike.output.file.field_separator"; // string
	public static final String OutputFileDataSeparator = "pike.output.file.data_separator"; // string

	public static final String OutputHdfsHost = "pike.output.hdfs.host"; // string
	public static final String OutputHdfsDirectory = "pike.output.hdfs.path"; // string
	public static final String OutputHdfsLocalDirectory = "pike.output.hdfs.localPath";	//string
	public static final String OutputHdfsSuffix = "pike.output.hdfs.suffix"; //string
	public static final String OutputHdfsFieldSeparator = "pike.output.hdfs.field_separator"; //string
	public static final String OutputHdfsDataSeparator = "pike.output.hdfs.data_separator"; //string
	public static final String OutputHdfsCompressed = "pike.output.hdfs.compressed"; //string

	public static final String OutputFSDirectory = "pike.output.fs.path"; // string

	public static final String OutputJdbcDriver = "pike.output.jdbc.driverClassName"; // string
	public static final String OutputJdbcDbUser = "pike.output.jdbc.dbUser"; // string
	public static final String OutputJdbcDbPassword = "pike.output.jdbc.dbPassword"; // string
	public static final String OutputJdbcDbUrl = "pike.output.jdbc.dbUrl"; // string
	public static final String OutputJdbcBatchInsertMaxCount = "pike.output.jdbc.batchinsert.maxcount"; // integer
	
	//sql server bulk
	public static final String SqlServerBulkClassName = "pike.output.sqlserverbulk.driverClassName"; // string
	public static final String SqlServerBulkUser = "pike.output.sqlserverbulk.dbUser"; // string
	public static final String SqlServerBulkPassword = "pike.output.sqlserverbulk.dbPassword"; // string
	public static final String SqlServerBulkDBUrl = "pike.output.sqlserverbulk.dbUrl"; // string
	public static final String SqlServerBulkWinDBFile = "pike.output.sqlserverbulk.winBulkPath"; // string
	public static final String SqlServerBulkLinuxDBFile = "pike.output.sqlserverbulk.linuxDBFilePath"; // string
	
	//hbase 
	public final static String HBaseKeysConfigKey = "pike.output.hbase.hbasekeys";
	public final static String HBaseTimestampConfigKey = "pike.output.hbase.timestampkey";
	
	public final static String ConsoleSubmiterIP = "pike.output.console.ip";
	public final static String ConsoleSubmiterPort = "pike.output.console.port";
	public final static String ConsoleSubmiterID = "pike.output.console.id";
	
	
	//Folder where store code table of cloud play, including country, area, city and isp
	public final static String CloudplayIPDictionaryDir = "pike.cloudplay.ip.dictionary.dir";
	
	//IP library Sync interval in minutes
	public final static String CloudplayIPSyncIntervalInMinutes = "pike.cloudplay.ip.sync.interval.minutes";	  

	// Kafka keys
    public final static String KafkaKeysConfigKey = "pike.output.kafka.kafkakeys";

    // Kafka broker host
    public final static String KafkaBrokerList = "kafka.metadata.broker.list";

	//public final static String XmlMetaDataFile = "pike.metadata.xmlfile";
	public final static String MetaDataProviderClass = "pike.metadata.provider.class";
	public final static String SpoutGeneratorClass = "pike.spout.generator.class";

	public final static String SpoutLocalTextFile = "pike.spout.local.textfile";
}
