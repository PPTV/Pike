package com.pplive.pike.exec.output;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.pplive.pike.base.Period;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import backtype.storm.Config;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.ISizeAwareIterable;
import com.pplive.pike.thriftgen.datatransfer.Row;
import com.pplive.pike.thriftgen.datatransfer.TransferService;

/// used for local debug/test
class ConsoleOutput implements IPikeOutput {

	@SuppressWarnings("rawtypes")
	private Map _pikeOrStormConf;
	private OutputSchema _outputSchema;
	private String targetName;
	private String ip;
	private int port;
	private String id;
	private String topologyName;

	public static final Logger log = LoggerFactory.getLogger(ConsoleOutput.class);

	@Override
	public void init(@SuppressWarnings("rawtypes") Map conf,
			OutputSchema oMetadata, String targetName, Period outputPeriod) {
		this._pikeOrStormConf = conf;
		this._outputSchema = oMetadata;
		this.targetName = targetName;
		this.topologyName = conf.get(Config.TOPOLOGY_NAME).toString();
		if (targetName.equalsIgnoreCase("submitter")) {
			log.info("console print submitter.");
			this.ip = conf.get(Configuration.ConsoleSubmiterIP).toString();
			this.port = Integer.parseInt(conf.get(
					Configuration.ConsoleSubmiterPort).toString());
			this.id = conf.get(Configuration.ConsoleSubmiterID).toString();
		} else {
			log.info("console print local.");
		}
	}

	@Override
	public void write(Calendar periodEnd,
			ISizeAwareIterable<List<Object>> tuples) {
		if (tuples == null) {
			return;
		}
		if (targetName.equalsIgnoreCase("local")) {
			writeHeader();
			for (List<Object> t : tuples) {
				System.out.println(StringUtils.join(t, '\t'));
			}
		} else {
			List<Row> rows = new ArrayList<Row>();
			for (List<Object> r : tuples) {
				Row row = new Row(new ArrayList<String>());
				for (Object v : r) {
					row.getColumns().add(v == null ? "" : v.toString());
				}
				rows.add(row);
			}
			if (!this.sendDataByThrift(periodEnd, rows)) {
				log.info("kill topology " + topologyName);
				this.execute(String.format("storm kill %s", this.topologyName));
			}
		}
	}

	private void execute(String command) {
		Process proc = null;
		BufferedReader output = null;
		InputStreamReader reader = null;
		try {

			proc = Runtime.getRuntime().exec(command);
			reader = new InputStreamReader(proc.getErrorStream(), "utf-8");

			output = new BufferedReader(reader);
			String line;
			while ((line = output.readLine()) != null) {
				log.info(line);
				System.out.println(line);
			}

			if (proc.waitFor() != 0) {
				log.error("execute command error: " + command);
			} else {
				log.info("execute command success: " + command);
			}

		} catch (IOException e) {
			log.error("execute command io error: " + command);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					log.error("reader close error.", e);
					reader = null;
				}
			}
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					log.error("reader close error.", e);
					output = null;
				}
			}
			if (proc != null) {
				proc.destroy();
			}
		}
	}

	private boolean sendDataByThrift(Calendar periodBegin, List<Row> rows) {
		int count = 0;
		while (count < 5) {
			if (count > 0) {
				try {
					Thread.sleep(count * 1000);
					log.info("sleep:" + count * 1000 + " ms");

				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			TTransport transport;
			try {
				log.info("sending data size:" + rows.size());
				transport = new TSocket(this.ip, this.port);
				TProtocol protocol = new TBinaryProtocol(transport);
				TransferService.Client client = new TransferService.Client(
						protocol);
				// Test.Client client = new Test.Client(protocol);
				transport.open();
				boolean success = client.send(rows, periodBegin.getTime()
						.getTime(), this.id);
				transport.close();
				log.info("send data complate.");
				return success;
			} catch (TTransportException e) {
				count++;
				log.info("thrift error.", e);
			} catch (TException e) {
				count++;
				log.info("thrift error.", e);
			}
		}
		return false;
	}

	public static void main(String[] args) {
		// pike.output.console.id : "f9c1a201-f5dc-4f05-a821-06e259d95880"
		// pike.output.console.ip : "172.16.3.7"
		// pike.output.console.port : 10000
		// pike.output.targets.default : "console.submitter"
		List<Row> rows = new ArrayList<Row>();
		for (int i = 0; i < 100; i++) {
			Row row = new Row(new ArrayList<String>());
			for (int j = 0; j < 10; j++) {
				row.getColumns().add(Integer.toString(i * j));
			}
			rows.add(row);
		}
		String ip = "172.16.3.7";
		int port = 10000;

		TTransport transport;
		try {
			log.info("sending data size:" + rows.size());
			transport = new TSocket(ip, port);
			TProtocol protocol = new TBinaryProtocol(transport);
			TransferService.Client client = new TransferService.Client(protocol);
			// Test.Client client = new Test.Client(protocol);
			transport.open();
			boolean success = client.send(rows, (new Date()).getTime(),
					"f9c1a201-f5dc-4f05-a821-06e259d95880");
			transport.close();
			log.info("send data complate.");
		} catch (TTransportException e) {
			log.info("thrift error.", e);
		} catch (TException e) {
			log.info("thrift error.", e);
		}

	}

	private void writeHeader() {
		final String topoName = Configuration.getString(this._pikeOrStormConf,
				Configuration.TOPOLOGY_NAME);
		final String jarVersion = Configuration.getString(
				this._pikeOrStormConf, Configuration.TopologyJarVersion);
		final String buildTime = Configuration.getString(this._pikeOrStormConf,
				Configuration.TopologyJarBuildTime);
		final String buildMachine = Configuration.getString(
				this._pikeOrStormConf, Configuration.TopologyJarBuildMachine);
		final String buildBy = Configuration.getString(this._pikeOrStormConf,
				Configuration.TopologyJarBuildBy);

		System.out.println(String.format("topo: %s [pike %s, %s, %s, %s]",
				topoName, jarVersion, buildTime, buildMachine, buildBy));

		StringBuffer sb = new StringBuffer();
		for (OutputField f : this._outputSchema.getOutputFields()) {
			sb.append(f.getName() + "\t");
		}
		System.out.println(sb.toString());
	}

	@Override
	public void close() throws IOException {

	}

}
