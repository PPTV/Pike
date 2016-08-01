package com.pplive.pike.function.builtin;

import com.pplive.pike.base.AbstractUDF;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Size extends AbstractUDF {

    private static final Pattern defaultPattern = Pattern.compile(",");

    public static Integer evaluate(Map<?,?> map) {
        return (map != null ? map.size() : 0);
    }

    public static Integer evaluate(String arrayVal) {
        return evaluate(arrayVal, ",");
    }

    public static Integer evaluate(String arrayVal, String separatorRegex) {
        if (arrayVal == null)
            return 0;
        final Pattern pattern;
        if (separatorRegex == null || separatorRegex.isEmpty() || separatorRegex.equals(",")) {
            pattern = defaultPattern;
        }
        else {
            pattern = Pattern.compile(separatorRegex);
        }

        int n = 1;
        Matcher matcher = pattern.matcher(arrayVal);
        while(matcher.find()) {
            if (matcher.end() < arrayVal.length()) {
                n += 1;
            }
        }
        return n;
    }

}
