package com.pplive.pike.parser;

public class SemanticErrorException extends RuntimeException {

    public SemanticErrorException() {
        super();
    }

    public SemanticErrorException(String message) {
        super(message);
    }

    public SemanticErrorException(String message, Throwable cause) {
        super(message, cause);
    }

    public SemanticErrorException(Throwable cause) {
        super(cause);
    }

}
