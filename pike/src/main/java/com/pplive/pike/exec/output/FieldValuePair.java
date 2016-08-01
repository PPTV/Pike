package com.pplive.pike.exec.output;

public class FieldValuePair {
    private final OutputField field;

    private final Object value;

    public FieldValuePair(OutputField field, Object value) {
        this.field = field;
        this.value = value;
    }

    public OutputField getField() {
        return field;
    }

    public Object getValue() {
        return value;
    }

}
