package com.pplive.pike.exec.spoutproto;

public class BinaryDeserializeException extends RuntimeException {

    public BinaryDeserializeException() {
        super();
    }

    public BinaryDeserializeException(String msg){
        super(msg);
    }

    public BinaryDeserializeException(String msg, Throwable cause){
        super(msg, cause);
    }

    public BinaryDeserializeException(Throwable cause){
        super(cause);
    }
}
