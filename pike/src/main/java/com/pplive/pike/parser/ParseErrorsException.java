package com.pplive.pike.parser;

import java.util.ArrayList;
import java.util.Collection;

public final class ParseErrorsException extends RuntimeException {

	private ArrayList<Exception> _exceptions;
	
    public ParseErrorsException(Exception exception) {
        super();
    	assert exception != null;
        this._exceptions = new ArrayList<Exception>(1);
        this._exceptions.add(exception);
    }
    
    public ParseErrorsException(Collection<Exception> exceptions) {
        super();
    	assert exceptions != null;
        this._exceptions = new ArrayList<Exception>(exceptions.size());
        this._exceptions.addAll(exceptions);
    }
    
    public void addParseErrors(Collection<Exception> exceptions) {
        this._exceptions.addAll(exceptions);
    }
    
    public void combineParseErrors(ParseErrorsException other) {
        this._exceptions.addAll(other._exceptions);
    }
    
    public Iterable<Exception> getParseErrors() {
    	return this._exceptions;
    }

}
