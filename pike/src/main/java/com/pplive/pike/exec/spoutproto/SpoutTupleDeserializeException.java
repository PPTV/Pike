package com.pplive.pike.exec.spoutproto;

public class SpoutTupleDeserializeException extends RuntimeException {

    public SpoutTupleDeserializeException() {
        super();
    }

    public SpoutTupleDeserializeException(String msg){
        super(msg);
    }

    public SpoutTupleDeserializeException(String msg, Throwable cause){
        super(msg, cause);
    }

    public SpoutTupleDeserializeException(Throwable cause){
        super(cause);
    }
}
