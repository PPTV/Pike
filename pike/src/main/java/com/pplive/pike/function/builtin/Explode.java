package com.pplive.pike.function.builtin;

import com.pplive.pike.base.AbstractUdtf;
import com.pplive.pike.exec.spoutproto.ColumnType;

import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

public class Explode extends AbstractUdtf{

    private static final Pattern defaultPattern = Pattern.compile(",");

    public static Object[][] evaluate(Map<?,?> map) {
        if (map == null) {
            return null;
        }

        ArrayList<Object[]> items = new ArrayList<Object[]>(map.size());
        for(Map.Entry<?,?> entry : map.entrySet()) {
            Object[] fields = new Object[2];
            fields[0] = entry.getKey();
            fields[1] = entry.getValue();
            items.add(fields);
        }
        return (Object[][])items.toArray();
    }

    public static String[] evaluate(String arrayVal) {
        return evaluate(arrayVal, ",");
    }

    public static String[] evaluate(String arrayVal, String separatorRegex) {
        if (arrayVal == null)
            return null;
        final Pattern pattern;
        if (separatorRegex == null || separatorRegex.equals(",")) {
            pattern = defaultPattern;
        }
        else {
            pattern = Pattern.compile(separatorRegex);
        }
        return pattern.split(arrayVal, -1);
    }

    public static String[][] evaluate(String arrayVal, String separatorRegex, String fieldSepRegex) {
        if (arrayVal == null)
            return null;
        final Pattern pattern;
        if (separatorRegex == null || separatorRegex.equals(",")) {
            pattern = defaultPattern;
        }
        else {
            pattern = Pattern.compile(separatorRegex);
        }

        String[] rows = pattern.split(arrayVal, -1);
        String[][] result = new String[rows.length][];
        for(int n = 0; n < rows.length; n +=1) {
            result[n] = evaluate(rows[n], fieldSepRegex);
        }
        return result;
    }

}
