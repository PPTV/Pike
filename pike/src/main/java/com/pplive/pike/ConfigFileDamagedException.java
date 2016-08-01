package com.pplive.pike;

public class ConfigFileDamagedException extends RuntimeException {

	private static final long serialVersionUID = -8538606102840133166L;
	
	public ConfigFileDamagedException(String filepath, String message) {
		this(filepath, message, null);
	}
	
	public ConfigFileDamagedException(String filepath, String message,
			Throwable cause) {
		super(String.format("this conf file %s is damaged,msg %s",
				filepath, message), cause);
	}

}
