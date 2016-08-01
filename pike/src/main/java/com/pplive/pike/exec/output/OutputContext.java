package com.pplive.pike.exec.output;

import com.pplive.pike.base.Period;
import com.pplive.pike.function.builtin.DateTime;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public final class OutputContext {

    private final Period _period;
    public Period getPeriod() {
        return this._period;
    }

    private Calendar _outputTime;
    public Calendar getOutputTime() {
        return this._outputTime;
    }

    public OutputContext(Period period) {
        this._period = period;
        setCurrentAsOutputTime();
    }

    public void setCurrentAsOutputTime() {
        this._outputTime = Calendar.getInstance();
    }

}
