package com.pplive.pike;

public class Program {

	public static void main(String[] args) {

		AppOptions appOptions = AppOptions.getInstance();
		if (appOptions.parseCommandlineOptions(args) == false) {
			appOptions.printHelp();
			System.exit(1);
		}
		assert appOptions.appCommand() != AppOptions.AppCommand.Unknown;
		int processExitCode = 0;

		switch (appOptions.appCommand()) {
		case Execute:
			processExitCode = new Driver().execute(appOptions);
			break;
		case Schema:
			processExitCode = new Driver().showSchema(appOptions);
			break;
		case TransferColumns:
			processExitCode = new Driver().transferColumns(appOptions);
			break;
		case PrintHelp:
			appOptions.printHelp();
			break;
		case ShowVersion:
			System.out
					.println(String
							.format("pike version %s, build time: %s (PPTV.com, pplive inc 2012 - 2014)",
									BuildInfo.BuildVersion, BuildInfo.BuildTime));
			break;
		case ShowBuildInfo:
			System.out
					.println(String
							.format("BuildVersion:%s, BuildTime:%s, BuildType:%s, BuildMachine:%s, BuildBy:%s",
									BuildInfo.BuildVersion,
									BuildInfo.BuildTime, BuildInfo.BuildType,
									BuildInfo.BuildMachine, BuildInfo.BuildBy));
			break;
		case PrintVersionNumber:
			System.out.println(BuildInfo.BuildVersion);
			break;
		case ShowTables:
			processExitCode = new Driver().showTables(appOptions);
			break;
		case DescTable:
			processExitCode = new Driver().descTable(appOptions);
			break;
		case ShowTopologies:
			processExitCode = new Driver().showTopologies(appOptions);
			break;
		case ShowTopologyConfig:
			processExitCode = new Driver().showTopologyConfig(appOptions);
			break;
		case ShowTopologySql:
			processExitCode = new Driver().showTopologySql(appOptions);
			break;
		case AddDbTable:
			processExitCode = new Driver().addDbTable(appOptions);
			break;
		}
		System.exit(processExitCode);
	}

}
