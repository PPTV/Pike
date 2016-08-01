package com.pplive.pike.function.builtin;

import com.pplive.pike.base.AbstractUDF;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

public class Math {
	public static class E extends AbstractUDF {
		private static final long serialVersionUID = 1L;
		
		private static final Double Val = java.lang.Math.E;
		public static Double evaluate() {
			return E.Val;
		}
	}
	
	public static class PI extends AbstractUDF {
		private static final long serialVersionUID = 1L;
		
		private static final Double Val = java.lang.Math.PI;
		public static Double evaluate() {
			return PI.Val;
		}
	}
	
	public static class Abs extends AbstractUDF {
		private static final long serialVersionUID = 1L;

		public static Integer evaluate(Integer val) {
			if (val == null)
				return null;
			return java.lang.Math.abs(val);
		}

		public static Long evaluate(Long val) {
			if (val == null)
				return null;
			return java.lang.Math.abs(val);
		}

		public static Float evaluate(Float val) {
			if (val == null)
				return null;
			return java.lang.Math.abs(val);
		}

		public static Double evaluate(Double val) {
			if (val == null)
				return null;
			return java.lang.Math.abs(val);
		}
	}

	public static class Ceil extends AbstractUDF {
		private static final long serialVersionUID = 1L;
		
		public static Long evaluate(Float val) {
			if (val == null)
				return null;
			return (long)java.lang.Math.ceil(val);
		}
		
		public static Long evaluate(Double val) {
			if (val == null)
				return null;
			return (long)java.lang.Math.ceil(val);
		}
	}

    public static class Floor extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Long evaluate(Float val) {
            if (val == null)
                return null;
            return (long)java.lang.Math.floor(val);
        }

        public static Long evaluate(Double val) {
            if (val == null)
                return null;
            return (long)java.lang.Math.floor(val);
        }
    }

    public static class Round extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Float evaluate(Float val) {
            return evaluate(val, null);
        }

        public static Float evaluate(Float val, Integer scale) {
            if (val == null)
                return null;
            if (val.isNaN() || val.isInfinite()) {
                return val;
            }
            if (scale == null || scale <= 0) {
                return (float)java.lang.Math.round(val);
            }

            return BigDecimal.valueOf(val).setScale(scale, RoundingMode.HALF_UP).floatValue();
        }

        public static Double evaluate(Double val) {
            return evaluate(val, null);
        }

        public static Double evaluate(Double val, Integer scale) {
            if (val == null)
                return null;
            if (val.isNaN() || val.isInfinite()) {
                return val;
            }
            if (scale == null || scale <= 0) {
                return (double)java.lang.Math.round(val);
            }

            return BigDecimal.valueOf(val).setScale(scale, RoundingMode.HALF_UP).doubleValue();
        }
    }

    public static class Rand extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        private Random _random = new Random();

        @Override
        public void init() {
            _random = new Random();
        }

        public Integer evaluate() {
            return _random.nextInt();
        }

        public Integer evaluate(Integer n) {
            return n != null ? _random.nextInt(n) : _random.nextInt();
        }
    }

}
