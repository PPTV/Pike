package com.pplive.pike.function.builtin;

import com.pplive.pike.base.AbstractUDF;

public class Struct {
	public static class GetField extends AbstractUDF {
		private static final long serialVersionUID = 1L;

        public static String evaluate(String structVal, Integer n) {
            return evaluate(structVal, n, ":");
        }

        public static String evaluate(String structVal, Integer n, String separator) {
            if (structVal == null)
                return null;
            if (n == null || n < 1)
                n = 1;
            if (separator == null)
                separator = ":";

            int count = 1;
            int nPosBegin = 0;
            int nPosEnd = structVal.indexOf(separator, nPosBegin);
            while (count < n && nPosEnd != -1) {
                count += 1;
                nPosBegin = nPosEnd + 1;
                nPosEnd = structVal.indexOf(separator, nPosBegin);
            }

            if (count < n || nPosBegin >= structVal.length()) {
                return null;
            }
            if (nPosEnd < 0) {
                nPosEnd = structVal.length();
            }
            return structVal.substring(nPosBegin, nPosEnd);
        }
	}

}
