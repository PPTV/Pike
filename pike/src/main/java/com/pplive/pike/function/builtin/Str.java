package com.pplive.pike.function.builtin;

import com.pplive.pike.base.AbstractUDF;

public class Str {

    public static class StrEqual extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Boolean evaluate(String left, String right) {
            if (left == null || right == null)
                return false;
            return left.equals(right);
        }
    }

    public static class StrNotEqual extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Boolean evaluate(String left, String right) {
            if (left == null || right == null)
                return false;
            return left.equals(right) == false;
        }
    }

    public static class StrCompare extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Integer evaluate(String left, String right) {
            if (left == null || right == null)
                return null;
            return left.compareTo(right);
        }
    }

    public static class StrLen extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Integer evaluate(String o) {
            if (o == null)
                return null;
            return o.length();
        }
    }

    public static class StrBeginWith extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Boolean evaluate(String s, String prefix) {
            if (s == null || prefix == null)
                return false;
            return s.startsWith(prefix);
        }
    }

    public static class StrEndsWith extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Boolean evaluate(String s, String suffix) {
            if (s == null || suffix == null)
                return false;
            return s.endsWith(suffix);
        }
    }

    public static class StrIsNullOrEmpty extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Boolean evaluate(String s) {
            return (s == null || s.isEmpty());
        }
    }

    public static class StrIsNotEmpty extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Boolean evaluate(String s) {
            return (s != null && s.isEmpty() == false);
        }
    }

    public static class StrLeft extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static String evaluate(String s, Integer count) {
            if (s == null || count == null)
                return null;
            return s.substring(0, count);
        }
    }

    public static class StrIndex extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Integer evaluate(String s, String sub) {
            if (s == null || sub == null)
                return null;
            return s.indexOf(sub);
        }
    }

    public static class StrSub extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static String evaluate(String s, Integer begin, Integer end) {
            if (s == null || begin == null)
                return null;
            if (end == null)
                return s.substring(begin);
            return s.substring(begin, end);
        }
    }
    
    public static class StrLastIndex extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Integer evaluate(String s, String sub) {
            if (s == null || sub == null)
                return null;
            return s.lastIndexOf(sub);
        }
    }

    public static class Concat extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static String evaluate(String s1, String s2) {
            StringBuilder sb = new StringBuilder();
            if (s1 != null && s1.length() > 0) {
                sb.append(s1);
            }
            if (s2 != null && s2.length() > 0) {
                sb.append(s2);
            }
            return sb.toString();
        }
    }

    public static class ConcatTriple extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static String evaluate(String s1, String s2, String s3) {
            StringBuilder sb = new StringBuilder();
            if (s1 != null && s1.length() > 0) {
                sb.append(s1);
            }
            if (s2 != null && s2.length() > 0) {
                sb.append(s2);
            }
            if (s3 != null && s3.length() > 0) {
                sb.append(s3);
            }
            return sb.toString();
        }
    }
}
