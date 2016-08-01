package com.pplive.pike.metadata;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.thriftgen.table.MetadataService;

/**
 * 表结构元数据提供
 * 
 * @author barryshen
 * 
 */
public class ThriftTableInfoProvider implements ITableInfoProvider {
	public ThriftTableInfoProvider(String thriftUrl) {
		this.thriftUrl = thriftUrl;
	}

	private final String thriftUrl;
	Logger logger = Logger.getLogger(ThriftTableInfoProvider.class);

	private TTransport parseTransport(String url) throws TTransportException {
		URI uri;
		try {
			uri = new URI(url);
		} catch (URISyntaxException e1) {
			logger.error(
					String.format("metadata thrift url(%s) config error", url),
					e1);
			throw new RuntimeException(String.format(
					"metadata thrift url(%s) config error", url), e1);
		}

		if (uri.getScheme().startsWith("http")) {
			return new THttpClient(url);
		} else if (uri.getScheme().startsWith("socket")) {
			return new TSocket(uri.getHost(), uri.getPort());
		} else {
			throw new RuntimeException("no support" + uri.toString());
		}
	}

	/**
	 * 根据表名获取表结构
	 * 
	 * @param name
	 *            表名
	 * @return 表结构
	 */
	public Table getTable(String name) {
		TTransport transport = null;

		try {
			transport = this.parseTransport(this.thriftUrl);
			TProtocol protocol = new TBinaryProtocol(transport);
			MetadataService.Client client = new MetadataService.Client(protocol);
			transport.open();
			return convert(client.getTable(name));
		} catch (Exception e) {
			logger.error(String.format("table name %s get error.", name), e);
			throw new RuntimeException(String.format(
					"table name %s get error.", name), e);

		} finally {
			if (transport != null)
				transport.close();
		}
	}

	private Table convert(com.pplive.pike.thriftgen.table.Table table) {
		int length = table.getColumns().size();
		Column[] columns = new Column[length];
		for (int i = 0; i < length; i++) {
			com.pplive.pike.thriftgen.table.Column c = table.getColumns()
					.get(i);
			Column column = new Column(c.getName(), c.getTitle(),
					convertColumnType(c.getColumnType()),
					c.getColumnTypeValue());
			columns[i] = column;
		}
		Table result = new Table(TableDataSource.Streaming, table.getName(),
				table.getTitle(), columns);
		return result;
	}

	private ColumnType convertColumnType(
			com.pplive.pike.thriftgen.table.ColumnType columnType) {
		switch (columnType) {
		case Boolean:
			return ColumnType.Boolean;
		case String:
			return ColumnType.String;
		case Int:
			return ColumnType.Int;
		case Double:
			return ColumnType.Double;
		case Long:
			return ColumnType.Long;
		case Float:
			return ColumnType.Float;
		case Complex:
			return ColumnType.String;
		case Map_ObjString:
			return ColumnType.Map_ObjString;
		case Byte:
			return ColumnType.Byte;
		case Short:
			return ColumnType.Short;
		case Date:
			return ColumnType.Date;
		case Time:
			return ColumnType.Time;
		case Timestamp:
			return ColumnType.Timestamp;
		default:
			throw new RuntimeException("no support " + columnType);
		}

	}

	/**
	 * 获取所有表名
	 * 
	 * @return 表列表
	 */
	public String[] getTableNames() {
		TTransport transport = null;

		try {
			transport = this.parseTransport(this.thriftUrl);
			TProtocol protocol = new TBinaryProtocol(transport);
			MetadataService.Client client = new MetadataService.Client(protocol);
			transport.open();

			List<String> tableNames = client.getTableNames();
			String[] result = new String[tableNames.size()];
			return tableNames.toArray(result);
		} catch (Exception e) {
			logger.error("get table name error", e);
			throw new RuntimeException("get table name error", e);

		} finally {
			if (transport != null)
				transport.close();
		}
	}

	/**
	 * 根据表名获取每小时的数据量
	 * 
	 * @param name
	 * @return
	 */
	public long getTableBytesByHour(String name) {
		TTransport transport = null;

		try {
			transport = this.parseTransport(this.thriftUrl);
			TProtocol protocol = new TBinaryProtocol(transport);
			MetadataService.Client client = new MetadataService.Client(protocol);
			transport.open();
			return client.getTableBytesByHour(name);
		} catch (Exception e) {
			logger.error("getTableBytesByHour error", e);
			throw new RuntimeException("getTableBytesByHour error", e);

		} finally {
			if (transport != null)
				transport.close();
		}
	}

	@Override
	public void registColumns(String id, String tableName, Set<String> columns) {
		TTransport transport = null;

		try {
			transport = this.parseTransport(this.thriftUrl);
			TProtocol protocol = new TBinaryProtocol(transport);
			MetadataService.Client client = new MetadataService.Client(protocol);
			transport.open();
			client.registColumns(id, tableName, columns);
		} catch (Exception e) {
			logger.error("getTableBytesByHour error", e);
			throw new RuntimeException("getTableBytesByHour error", e);

		} finally {
			if (transport != null)
				transport.close();
		}

	}
}
