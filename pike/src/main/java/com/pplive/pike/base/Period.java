package com.pplive.pike.base;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Immutable
public class Period implements Serializable{
	
	private static final long serialVersionUID = -6235204288443894819L;

    private final static Logger logger = Logger.getLogger(Period.class);

	private final int _periodSeconds;
	public int periodSeconds() { return this._periodSeconds; }

    private final int _beginOffsetSeconds;
    public int periodBeginOffsetSeconds() { return this._beginOffsetSeconds; }

	public static Period secondsOf(int periodSeconds){
		return new Period(periodSeconds, 0);
	}

    private static final Pattern _periodPattern = Pattern.compile("^(\\d+)(s|S|m|M|h|H|d|D)\\s*(,\\s*(\\d+)(s|S|m|M|h|H))?$");
    public static Period parse(String periodString){
        if (periodString == null || periodString.isEmpty()){
            throw new IllegalArgumentException("periodString cannot be empty");
        }
        Matcher match = _periodPattern.matcher(periodString);
        if (match.matches() == false) {
            return null;
        }

        int seconds = Integer.valueOf(match.group(1));
        char c = match.group(2).charAt(0);
        if(c == 'm' || c == 'M'){
            seconds *= 60;
        }
        else if(c == 'h' || c == 'H'){
            seconds *= 3600;
        }
        else if(c == 'd' || c == 'D'){
            seconds *= 3600 * 24;
        }

        String s = match.group(4);
        if (s == null || s.isEmpty()) {
            return new Period(seconds, 0);
        }

        int offset = Integer.valueOf(s);
        c = match.group(5).charAt(0);
        if(c == 'm' || c == 'M'){
            offset *= 60;
        }
        else if(c == 'h' || c == 'H'){
            offset *= 3600;
        }

        return new Period(seconds, offset);
    }
	
	private Period(int periodSeconds, int beginOffsetSeconds)
	{
        if (periodSeconds <= 0){
            throw new IllegalArgumentException("periodSeconds must be > 0");
        }
        if (beginOffsetSeconds < 0){
            throw new IllegalArgumentException("beginOffsetSeconds must be > 0");
        }
		this._periodSeconds = periodSeconds;
        this._beginOffsetSeconds = beginOffsetSeconds;
	}
	
	@Override
    public String toString(){
        if (this._beginOffsetSeconds == 0) {
            return toDurationString(this._periodSeconds);
        }
        else {
            return String.format("%s, begin offset %s", toDurationString(this._periodSeconds), toDurationString(this._beginOffsetSeconds));
        }
    }
    private static String toDurationString(int seconds){
        if (seconds < 60)
            return String.format("%d seconds", seconds);
        if (seconds < 3600)
            return String.format("%d minutes", seconds/60);
        return String.format("%d hours", seconds/3600);
    }


    public static boolean nowStillBeforePeriodEnd(Calendar periodEnd) {
		return Calendar.getInstance().before(periodEnd);
	}
	
	public static void moveCalendarTime(Calendar t, Period period, int count) {
		t.add(Calendar.SECOND, count * period._periodSeconds);
	}
	
	public Calendar previousPeriodBegin() 
	{
		Calendar time = currentPeriodBegin();
		time.add(Calendar.SECOND, -this._periodSeconds);
		return time;
	}

    public Calendar currentPeriodBegin()
    {
        Calendar time = Calendar.getInstance();
        return this.periodBegin(time, 0);
    }

    public Calendar currentPeriodEnd() {
        Calendar time = currentPeriodBegin();
        time.add(Calendar.SECOND, this._periodSeconds);
        return time;
    }

    public Calendar currentPeriodBegin(int tolerantDelayMilliseconds)
    {
        Calendar time = Calendar.getInstance();
        time.add(Calendar.SECOND, -this._beginOffsetSeconds);
        Calendar begin = this.periodBegin(time, tolerantDelayMilliseconds);
        begin.add(Calendar.SECOND, this._beginOffsetSeconds);
        return begin;
    }

    private static Long timeToLong(Calendar t) {
        assert t != null;
        return t.get(Calendar.YEAR) * 10000000000L
                + (t.get(Calendar.MONTH) + 1) * 100000000L
                + t.get(Calendar.DAY_OF_MONTH) * 1000000L
                + t.get(Calendar.HOUR_OF_DAY) * 10000
                + t.get(Calendar.MINUTE) * 100 + t.get(Calendar.SECOND);
    }

    public Calendar currentPeriodEnd(int tolerantDelayMilliseconds) {

        Calendar time = currentPeriodBegin(tolerantDelayMilliseconds);
        time.add(Calendar.SECOND, this._periodSeconds);
        return time;
    }

    public Calendar periodBegin(Calendar time) {
        return periodBegin(time, 0);
    }

    public Calendar periodBegin(Calendar time, int tolerantDelayMilliseconds) {
        if(time == null){
            throw new IllegalArgumentException("time cannot be null");
        }

        time = (Calendar)time.clone();
        if (tolerantDelayMilliseconds != 0) {
            time.add(Calendar.MILLISECOND, -tolerantDelayMilliseconds);
        }

        if (this._periodSeconds >= 3600) {
            assert this._periodSeconds % 3600 == 0;
            int periodHours = this._periodSeconds / 3600;
            int hours = time.get(Calendar.HOUR_OF_DAY);
            time.set(Calendar.HOUR_OF_DAY, hours - (hours % periodHours));
            time.set(Calendar.MINUTE, 0);
            time.set(Calendar.SECOND, 0);
            time.set(Calendar.MILLISECOND, 0);
        }
        else if (this._periodSeconds >= 60) {
            int periodMinutes = this._periodSeconds / 60;
            int minutes = time.get(Calendar.MINUTE);
            time.set(Calendar.MINUTE, minutes - (minutes % periodMinutes));
            time.set(Calendar.SECOND, 0);
            time.set(Calendar.MILLISECOND, 0);
        }
        else {
            int seconds = time.get(Calendar.SECOND);
            time.set(Calendar.SECOND, seconds - (seconds % this._periodSeconds));
            time.set(Calendar.MILLISECOND, 0);
        }
        return time;
    }

    public Calendar periodEnd(Calendar t) {
        return periodEnd(t, 0);
    }

    public Calendar periodEnd(Calendar t, int tolerantDelayMilliseconds) {
        Calendar time = periodBegin(t, tolerantDelayMilliseconds);
        time.add(Calendar.SECOND, this._periodSeconds);
        return time;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null){
            return false;
        }
        if (o.getClass() != Period.class){
            return false;
        }
        return equals((Period)o);
    }

    public boolean equals(Period other) {
        if (other == null){
            return false;
        }
        return this._periodSeconds == other._periodSeconds
                && this._beginOffsetSeconds == other._beginOffsetSeconds;
    }

    @Override
    public int hashCode() {
        return this._periodSeconds;
    }
}
