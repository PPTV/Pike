package com.pplive.pike.exec.output;

import java.io.Serializable;

public enum OutputType implements Serializable {

	Unknown, Console, SocketServer, File, Jdbc, HBase, SQLServerBulk, Hdfs, Kafka;

	public static OutputType parse(String name) {
		for (OutputType type : OutputType.values()) {
			if (type.toString().equalsIgnoreCase(name))
				return type;
		}
		return Unknown;
	}
}
