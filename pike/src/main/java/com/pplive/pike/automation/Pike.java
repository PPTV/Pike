package com.pplive.pike.automation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.generated.TopologySummary;

import com.pplive.pike.AppOptions;
import com.pplive.pike.Configuration;
import com.pplive.pike.PikeSqlCompiler;
import com.pplive.pike.base.PikeTopologyClient;
import com.pplive.pike.exec.output.OutputField;
import com.pplive.pike.exec.output.OutputSchema;
import com.pplive.pike.exec.output.OutputTarget;
import com.pplive.pike.metadata.TableManager;
import com.pplive.pike.parser.LogicalQueryPlan;
import com.pplive.pike.parser.ParseErrorsException;
import com.pplive.pike.parser.PikeSqlParser;
import com.pplive.pike.parser.SemanticErrorException;

import com.pplive.pike.util.CollectionUtil;
import net.sf.jsqlparser.JSQLParserException;

public class Pike {

	private final String _nimbusHost;
	private final int _nimbusThriftPort;

	public Pike(String stormNimbusHost, int stormNimbusThriftPort) {
		if (stormNimbusHost == null || stormNimbusHost.isEmpty()) {
			throw new IllegalArgumentException(
					"stormNimbusHost cannot be null or empty");
		}
		if (stormNimbusThriftPort <= 0) {
			throw new IllegalArgumentException(
					"stormNimbusThriftPort must be positive integer");
		}
		this._nimbusHost = stormNimbusHost;
		this._nimbusThriftPort = stormNimbusThriftPort;
	}
	public Pike(){
		Configuration conf = Configuration.getStormConf();
		this._nimbusHost = Configuration.getString(conf, Config.NIMBUS_HOST);
        this._nimbusThriftPort = Configuration.getInt(conf, Config.NIMBUS_THRIFT_PORT, 6627);
	}

	public List<TopologySummary> getRunningTopologies() {
		
		PikeTopologyClient client = new PikeTopologyClient(this._nimbusHost,
				this._nimbusThriftPort);
		List<TopologySummary> topologies = client.getRunningTopologies();
		return topologies;
	}

	public Map getTopologyConfig(String topologyName) {
		PikeTopologyClient client = new PikeTopologyClient(this._nimbusHost,
				this._nimbusThriftPort);
		String id = client.getRunningTopologyIdByName(topologyName);
		if (id.isEmpty()) {
			return null;
		}
		Map topologyConf = client.getTopologyConf(id);
		return topologyConf;
	}

	public String getTopologySql(String topologyName) {
		if (topologyName == null || topologyName.isEmpty())
			throw new IllegalArgumentException("topologyName cannot be empty");
		@SuppressWarnings("rawtypes")
		Map topologyConf = getTopologyConfig(topologyName);
		if (topologyConf == null) {
			return "";
		}
		String sql = Configuration.getString(topologyConf,
                Configuration.TopologySql);
		return sql;
	}

	public static void validateSQLSyntax(String sql) throws JSQLParserException {
		if (sql == null)
			throw new IllegalArgumentException("sql cannot be null");
		PikeSqlParser.parseSQLSyntax(sql);
	}

	public static Map<String, Object> parseOptionsInSql(String sql)
			throws JSQLParserException {
		if (sql == null)
			throw new IllegalArgumentException("sql cannot be null");
		Map<String, Object> result = PikeSqlParser.parseSQLOptions(sql);
		return result;
	}

	public static OutputSchema getOutputSchema(LogicalQueryPlan queryPlan) {
		if (queryPlan == null)
			throw new IllegalArgumentException("queryPlan cannot be null");
		final ArrayList<OutputField> outputFields = queryPlan.getOutputFields();
		final OutputSchema outputSchema = new OutputSchema("", outputFields);
		return outputSchema;
	}

	public static OutputSchema getOutputSchema(String sql) {
		return getOutputSchema(sql, null);
	}

	public static OutputSchema getOutputSchema(String sql,
			Map<String, Object> conf) {
		return getOutputSchema(parseSQL(sql, conf));
	}

	public static OutputSchema getOutputSchema(String sql,
			Map<String, Object> conf, boolean optimize) {
		return getOutputSchema(parseSQL(sql, conf, optimize));
	}

    public static ArrayList<OutputTarget> getOutputTargets(LogicalQueryPlan queryPlan) {
        if (queryPlan == null)
            throw new IllegalArgumentException("queryPlan cannot be null");
        return CollectionUtil.copyArrayList(queryPlan.getOutputTargets());
    }

    public static ArrayList<OutputTarget> getOutputTargets(String sql) {
        return getOutputTargets(sql, null);
    }

    public static ArrayList<OutputTarget> getOutputTargets(String sql, Map<String, Object> conf) {
        return getOutputTargets(parseSQL(sql, conf));
    }

    public static ArrayList<OutputTarget> getOutputTargets(String sql,
                  Map<String, Object> conf, boolean optimize) {
        return getOutputTargets(parseSQL(sql, conf, optimize));
    }

    public static LogicalQueryPlan parseSQL(String sql, Map<String, Object> conf) {
		return parseSQL(sql, conf, true);
	}

	public static LogicalQueryPlan parseSQL(String sql,
			Map<String, Object> conf, boolean optimize) {
		if (sql == null)
			throw new IllegalArgumentException("sql cannot be null");

		Configuration config = new Configuration();
		if (conf != null) {
			for (Map.Entry<String, Object> kv : conf.entrySet()) {
				config.put(kv.getKey(), kv.getValue());
			}
		}
		TableManager tableManager = new TableManager(config);

		try {
			LogicalQueryPlan queryPlan = PikeSqlParser.parseSQL(sql,
					tableManager);
			if (optimize) {
				queryPlan.optimize();
			}
			return queryPlan;
		} catch (JSQLParserException e) {
			throw new RuntimeException("sql syntax error", e);
		} catch (SemanticErrorException e) {
			throw new RuntimeException("sql semantic error", e);
		} catch (UnsupportedOperationException e) {
			throw new RuntimeException("sql implementation problem", e);
		} catch (ParseErrorsException e) {
			throw new RuntimeException("there are errors", e);
		}
	}

	// this method requires the calling program is running on machine with storm
	// installed.
	public static void compileAndSubmit(String topologyName, String sql,
			Map<String, Object> conf) {
		String[] args = new String[] { "-exec", "-n", "<topologyName>", "-e",
				"<sql>" };
		args[2] = topologyName;
		args[4] = sql;
		AppOptions appOptions = AppOptions.newInstance();
		appOptions.parseCommandlineOptions(args);
		PikeSqlCompiler.compileAndSubmit(topologyName, sql, appOptions, conf);
	}
	public static void killTopology(String topologyName){
		
	}

}
