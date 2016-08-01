package com.pplive.pike.generator;

public class TopologyNotSupportedException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public TopologyNotSupportedException() {
        super();
    }

    public TopologyNotSupportedException(String message) {
        super(message);
    }

    public TopologyNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public TopologyNotSupportedException(Throwable cause) {
        super(cause);
    }

}
