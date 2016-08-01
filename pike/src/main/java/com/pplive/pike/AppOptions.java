package com.pplive.pike;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class AppOptions {
	public static enum AppCommand {
		Unknown, Execute, PrintHelp, ShowVersion, ShowBuildInfo, PrintVersionNumber, ShowTables, DescTable, ShowTopologies, ShowTopologyConfig, ShowTopologySql, AddDbTable, Schema,TransferColumns
	}

	private static final AppOptions _instance = new AppOptions();

	public static AppOptions getInstance() {
		return AppOptions._instance;
	}

	public static AppOptions newInstance() {
		return new AppOptions();
	}

	private final Options _options;
	private CommandLine _commandLine;
	private AppCommand _action;

	public AppCommand appCommand() {
		return this._action;
	}

	public String tableName() {
		String[] args = this._commandLine.getArgs();
		if (args.length == 0) {
			return "";
		}
		return args[0];
	}

	public String topologyName() {
		return getStringOption("n", "");
	}

	public String commandlineSQL() {
		return getStringOption("e", "");
	}

	public String sqlFile() {
		return getStringOption("f", "");
	}

	public boolean isDebugTopology() {
		return this._commandLine.hasOption("td");
	}

	public boolean isParseMode() {
		return this._commandLine.hasOption("parse");
	}

	public boolean isCompileMode() {
		return this._commandLine.hasOption("compile");
	}

	public boolean isExplainMode() {
		return this._commandLine.hasOption("explain");
	}

	public boolean isGenerateMode() {
		return this._commandLine.hasOption("gentopo");
	}

	public boolean isDisplayTridentGraph() {
		return this._commandLine.hasOption("tridentgraph");
	}

	public boolean isRunMode() {
		return (isParseMode() || isCompileMode() || isExplainMode()
				|| isGenerateMode() || isDisplayTridentGraph()) == false;
	}

	public boolean isLocalMode() {
		return this._commandLine != null
				&& this._commandLine.hasOption("local");
	}

	public boolean isConsolePrint() {
		return this._commandLine != null && this._commandLine.hasOption("p");
	}

	public boolean disableOptimize() {
		return this._commandLine.hasOption("nooptimize");
	}

	public String tableInfoFile() {
		return getStringOption("ltf", "");
	}

	public String tableDataFile() {
		return getStringOption("ldf", "");
	}

	public int localModeRunSeconds() {
		return getIntOption("lsec", 0);
	}

	public Properties getOverridedTopologyConfig() {
		return this._commandLine.getOptionProperties("D");
	}

	private int getIntOption(String name, int defaultValue) {
		assert name != null && name.length() > 0;

		String s = this._commandLine.getOptionValue(name);
		if (s == null) {
			return defaultValue;
		}
		return Integer.parseInt(s);
	}

	private String getStringOption(String name, String defaultValue) {
		assert name != null && name.length() > 0;

		String s = this._commandLine.getOptionValue(name);
		if (s == null) {
			return defaultValue;
		}
		return s;
	}

	public boolean parseCommandlineOptions(String[] args) {
		CommandLineParser parser = new GnuParser();
		if (args.length == 0) {
			return false;
		}
		this._action = parseAction(args[0]);
		if (this._action == AppCommand.Unknown) {
			System.err.println("unknown action: " + args[0]);
			return false;
		}

		try {
			String[] optionArgs = Arrays.copyOfRange(args, 1, args.length);
			this._commandLine = parser.parse(this._options, optionArgs);
		} catch (ParseException exp) {
			System.err.println("Invalid command line options: "
					+ exp.getMessage());
			return false;
		}

		return checkOptionsRequirement();
	}

	private boolean checkOptionsRequirement() {
		switch (this._action) {
		case Execute:
			if (this.commandlineSQL().isEmpty() && this.sqlFile().isEmpty()) {
				System.err
						.println("argument error: exec action must specify sql string or sql file (-e or -f)");
				return false;
			}
			int n = 0;
			if (this.isParseMode())
				n += 1;
			if (this.isCompileMode())
				n += 1;
			if (this.isExplainMode())
				n += 1;
			if (this.isDisplayTridentGraph())
				n += 1;
			if (this.isGenerateMode())
				n += 1;
			if (n > 1) {
				System.err
						.println("argument error: can only specify one option in -parse, -compile, -explain , -tridentgraph and -gentopo");
				return false;
			}
			if (this.isLocalMode() == false && isRunMode()
					&& !checkTopoNameValid(this.topologyName())) {
				System.err
						.println("argument error: must use -n option to specify topology name which must be english character,number or  like '-' or '_' if submit to storm cluster running.");
				return false;
			}
			if (this.isLocalMode() && isRunMode()
					&& this.localModeRunSeconds() < 0) {
				System.err
						.println("argument error: -lsec must specify positive integer.");
				return false;
			}
			break;
		case Schema:
		case TransferColumns:
			if (this.commandlineSQL().isEmpty() && this.sqlFile().isEmpty()) {
				System.err
						.println("argument error: schema or transferColumns action must specify sql string or sql file (-e or -f)");
				return false;
			}
			break;
		case DescTable:
			if (this.tableName().isEmpty()) {
				System.err
						.println("argument error: desc action must specify table name");
				return false;
			}
			break;
		case ShowTopologyConfig:
		case ShowTopologySql:
			if (this.topologyName().isEmpty()) {
				System.err
						.println("argument error: must specify topology name (-n)");
				return false;
			}
			break;
		case AddDbTable:
			if (this.commandlineSQL().isEmpty()) {
				System.err
						.println("argument error: must specify create table sql string (-e)");
				return false;
			}
			if (checkCreateTableSql(this.commandlineSQL()) == false) {
				System.err
						.println("argument error: SQL is not 'create table ...' or contains sensitive keywords: drop|truncate|delete|insert|replace|update");
				return false;
			}
			break;
		}
		return true;
	}

	private static boolean checkCreateTableSql(String sql) {
		assert sql != null;
		final Pattern pattern1 = Pattern.compile("^\\s*create\\s+table\\s+",
				Pattern.CASE_INSENSITIVE);
		final Pattern pattern2 = Pattern.compile(
				"\\s(drop|truncate|delete|insert|replace|update)\\s",
				Pattern.CASE_INSENSITIVE);
		if (pattern1.matcher(sql).find() == false) {
			return false;
		}
		if (pattern2.matcher(sql).find()) {
			return false;
		}
		return true;
	}
	
	private static boolean checkTopoNameValid(String name) {
		String pattern = "[\\w-]+";
		return name.matches(pattern);
	}
	
	public void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(120);
		System.out.println("pike <command> <options>");
		System.out.println("commands:");
		System.out
				.println("-exec \t\t\tgenerate topology from SQL and submit it to storm cluster");
		System.out.println("-h \t\t\tprint this help");
		System.out.println("-v \t\t\tshow pike version info");
		System.out.println("-schema \t\t\tgenerate schema from SQL");
		System.out.println("-transferColumns \t\t\tgenerate transfer columns from SQL");
		System.out.println("-version \t\tshow pike version pure number string");
		System.out.println("-showtables \t\tshow all table name");
		System.out
				.println("-desc <table name>\tshow columns of the specified table");
		System.out.println("-showtopos \t\tshow all running topologies");
		System.out
				.println("-topoconf -n <topology name>\tshow topology running configuration");
		System.out.println("-toposql -n <topology name>\tshow topology sql");
		System.out
				.println("-adddbtable -e <sql string>\tadd new table to db used by jdbc output");
		formatter.printHelp("options", this._options);
	}

	private static AppCommand parseAction(String action) {
		if (action.equals("-exec"))
			return AppCommand.Execute;
		if (action.equals("-h"))
			return AppCommand.PrintHelp;
		if (action.equals("-v"))
			return AppCommand.ShowVersion;
		if (action.equals("-version"))
			return AppCommand.PrintVersionNumber;
		if (action.equals("-buildinfo"))
			return AppCommand.ShowBuildInfo;
		if (action.equals("-showtables"))
			return AppCommand.ShowTables;
		if (action.equals("-desc"))
			return AppCommand.DescTable;
		if (action.equals("-showtopos"))
			return AppCommand.ShowTopologies;
		if (action.equals("-topoconf"))
			return AppCommand.ShowTopologyConfig;
		if (action.equals("-toposql"))
			return AppCommand.ShowTopologySql;
		if (action.equals("-adddbtable"))
			return AppCommand.AddDbTable;
		if (action.equals("-schema"))
			return AppCommand.Schema;
		if(action.equalsIgnoreCase("-transferColumns"))
			return AppCommand.TransferColumns;
		return AppCommand.Unknown;
	}

	@SuppressWarnings("static-access")
	private AppOptions() {
		Options options = new Options();

		Option option = OptionBuilder.hasArg()
				.withDescription("read pike SQL from command line").create("e");
		option.setLongOpt("sql");
		options.addOption(option);

		option = OptionBuilder.hasArg()
				.withDescription("read pike SQL from file").create("f");
		option.setLongOpt("sqlfile");
		options.addOption(option);

		option = OptionBuilder.hasArg()
				.withDescription("read pike SQL from file").create("n");
		option.setLongOpt("topologyname");
		options.addOption(option);

		option = OptionBuilder.withDescription(
				"no optimize when use -explain to show plan").create(
				"nooptimize");
		options.addOption(option);

		option = OptionBuilder.withDescription(
				"only parse SQL, don't do semantic analyzing").create("parse");
		options.addOption(option);

		option = OptionBuilder.withDescription("only compile SQL, no submit")
				.create("compile");
		options.addOption(option);

		option = OptionBuilder.withDescription(
				"show SQL compiled logical query plan, no submit").create(
				"explain");
		options.addOption(option);

		option = OptionBuilder
				.withDescription(
						"compile SQL and generate storm trident plan, display the graph, no submit")
				.create("tridentgraph");
		options.addOption(option);

		option = OptionBuilder.withDescription(
				"compile SQL and generate storm topoloty, no submit").create(
				"gentopo");
		options.addOption(option);

		option = OptionBuilder.withDescription("local mode").create("local");
		options.addOption(option);
		option = OptionBuilder.withDescription("print submitter console.")
				.create("p");
		options.addOption(option);

		option = OptionBuilder.hasArg()
				.withDescription("table info xml file in local mode")
				.create("ltf");
		options.addOption(option);

		option = OptionBuilder.hasArg()
				.withDescription("table data file in local mode").create("ldf");
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withDescription(
						"local mode run seconds before exit, default is unlimit")
				.create("lsec");
		options.addOption(option);

		option = OptionBuilder.withDescription(
				"set generated topology in debug mode").create("td");
		option.setLongOpt("topology-debug");
		options.addOption(option);

		option = OptionBuilder
				.withArgName("property=value")
				.hasArgs(2)
				.withValueSeparator()
				.withDescription("override sql/topology configuration property")
				.create("D");
		options.addOption(option);

		this._options = options;
	}

}
