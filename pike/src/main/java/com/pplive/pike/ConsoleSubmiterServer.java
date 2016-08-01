package com.pplive.pike;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.pplive.pike.metadata.TableManager;
import com.pplive.pike.thriftgen.datatransfer.Row;
import com.pplive.pike.thriftgen.datatransfer.TransferService;

public class ConsoleSubmiterServer {
	public static final Log log = LogFactory
			.getLog(ConsoleSubmiterServer.class);

	public ConsoleSubmiterServer() {
		try {
			this.ip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			throw new RuntimeException("get host address fail", e);
		}
		this.port = this.getFreePort();
		this.id = UUID.randomUUID().toString();
	}

	static final int MINPORT = 10000;
	static final int MAXPORT = 65000;

	private String ip;
	private int port;

	public int getPort() {
		return this.port;
	}

	private String id;

	public String getID() {
		return this.id;
	}

	public Map<String, Object> getThriftConf() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Configuration.OutputDefaultTargets, "console.submitter");
		conf.put(Configuration.ConsoleSubmiterIP, this.ip);
		conf.put(Configuration.ConsoleSubmiterPort, this.port);
		conf.put(Configuration.ConsoleSubmiterID, this.id);
		return conf;
	}

	// 获取通信端口号
	public int getFreePort() {

		for (int i = MINPORT; i < MAXPORT; i++) {
			try {
				ServerSocket socket = new ServerSocket(i);
				socket.close();
				return i;
			} catch (IOException e) {
				continue;
			}

		}
		throw new RuntimeException("not found free port");
	}

	public void registColumns(Configuration config, String tableName,
			Set<String> columns) {
		Timer timer = new Timer();
		timer.schedule(new Task(this.id, config, tableName, columns),
				new Date(), 60000);
	}

	private static class Task extends TimerTask {
		public Task(String id, Configuration config, String tableName,
				Set<String> columns) {
			this.id = id;
			this.config = config;
			this.tableName = tableName;
			this.columns = columns;
		}

		private final String id;
		private final Configuration config;
		private final String tableName;
		private final Set<String> columns;

		@Override
		public void run() {
//			log.info(String.format("registColumns id %s tableName %s:%s",
//					this.id, this.tableName,
//					StringUtils.join(this.columns, ",")));
			TableManager manager = new TableManager(this.config);
			manager.registColumns(this.id, this.tableName, this.columns);

		}

	}

	public void waitForResult() {
		this.start();
	}

	private void start() {
		TServerSocket serverTransport;
		try {
			serverTransport = new TServerSocket(this.port);

			@SuppressWarnings({ "rawtypes", "unchecked" })
			TransferService.Processor processor = new TransferService.Processor(
					new Transfer(this.id));
			Factory protFactory = new TBinaryProtocol.Factory(true, true);
			org.apache.thrift.server.TThreadPoolServer.Args args = new org.apache.thrift.server.TThreadPoolServer.Args(
					serverTransport);
			args.processor(processor);
			args.protocolFactory(protFactory);
			TServer server = new TThreadPoolServer(args);
			server.serve();
		} catch (TTransportException e) {
			throw new RuntimeException("server error.", e);
		}
	}

	private class Transfer implements TransferService.Iface {
		public Transfer(String id) {
			this.id = id;
		}

		private final String id;

		@Override
		public boolean send(List<Row> rows, long dateTime, String id)
				throws TException {
			if (!this.id.equals(id)) {
				// System.out.println(String.format(
				// "\nthis id is %s,but message id is %s,don't accept.",
				// this.id, id));
				return false;
			}
			System.out.println("--------------"
					+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
							Locale.ENGLISH).format(new Date(dateTime))
					+ "-----------------");
			System.out.println();
			for (Row row : rows) {
				for (String v : row.getColumns()) {
					System.out.print(v + "\t");
				}
				System.out.println();
			}
			System.out.println();
			return true;

		}

	}

}
