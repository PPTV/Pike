package com.pplive.pike.function.builtin;

import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.pplive.pike.base.AbstractUDF;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.output.OutputContext;
import org.apache.log4j.Logger;

public class DateTime {
    private final static Logger logger = Logger.getLogger(DateTime.class);

    public static class CurDate extends AbstractUDF {
		private static final long serialVersionUID = 1L;

		public static Date evaluate() {
			return new Date(System.currentTimeMillis());
		}
	}

	public static class CurTime extends AbstractUDF {
		private static final long serialVersionUID = 1L;

		public static Time evaluate() {
			return new Time(System.currentTimeMillis());
		}
	}

    public static int decideTolerantDelayMilliseconds(Period period) {
        return decideTolerantDelayMilliseconds(period.periodSeconds());
    }

    public static int decideTolerantDelayMilliseconds(int periodSeconds) {
        int seconds = periodSeconds;
        if (seconds <= 6){
            if (seconds <= 1)
                return 500;
            else if (seconds <= 3)
                return 1000;
            else
                return 2000;
        }
        else {
            if (seconds <= 20)
                return 3000;
            else if (seconds <= 180)
                return 15000;
            else
                return 20000;
        }
    }

    private static Calendar periodBegin(OutputContext context, Integer tolerantDelayMilliseconds) {
        if (context == null) {
            return null;
        }
        final Period period = context.getPeriod();
        final Calendar outputTime = context.getOutputTime();
        final int ms = tolerantDelayMilliseconds != null ? tolerantDelayMilliseconds : decideTolerantDelayMilliseconds(period);
        final Calendar t = period.periodBegin(outputTime, ms);
        return t;
    }

    private static Calendar periodEnd(OutputContext context, Integer tolerantDelayMilliseconds) {
        if (context == null) {
            return null;
        }
        final Period period = context.getPeriod();
        final Calendar outputTime = context.getOutputTime();
        final int ms = tolerantDelayMilliseconds != null ? tolerantDelayMilliseconds : decideTolerantDelayMilliseconds(period);
        final Calendar t = period.periodEnd(outputTime, ms);
        return t;
    }

    public static class CurPeriodBeginDate extends AbstractUDF {
		private static final long serialVersionUID = 1L;

        public static Date evaluate(OutputContext context) {
            return evaluate(context, null);
        }

        public static Date evaluate(OutputContext context, Integer tolerantDelayMilliseconds) {
            Calendar t = periodBegin(context, tolerantDelayMilliseconds);
            return t == null ? null : new Date(t.getTimeInMillis());
        }

        public static Date evaluate(String period) {
            return evaluate(period, null);
        }

        public static Date evaluate(Integer periodSeconds) {
			return evaluate(periodSeconds, null);
		}

		public static Date evaluate(String period,
				Integer tolerantDelayMilliseconds) {
			if (period == null || period.isEmpty())
				return null;
			Period p = Period.parse(period);
			if (p == null)
				return null;
			if (tolerantDelayMilliseconds == null) {
				tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(p);
			}
			return eval(p, tolerantDelayMilliseconds);
		}

		public static Date evaluate(Integer periodSeconds,
				Integer tolerantDelayMilliseconds) {
			if (periodSeconds == null) {
				return null;
			}
			if (tolerantDelayMilliseconds == null) {
				tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(periodSeconds);
			}
			return eval(Period.secondsOf(periodSeconds),
					tolerantDelayMilliseconds);
		}

		private static Date eval(Period period, int tolerantDelayMilliseconds) {
			assert period != null;

			Calendar t = period.currentPeriodBegin(tolerantDelayMilliseconds);
			return new Date(t.getTimeInMillis());
		}
	}

    public static class CurPeriodEndDate extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Date evaluate(OutputContext context) {
            return evaluate(context, null);
        }

        public static Date evaluate(OutputContext context, Integer tolerantDelayMilliseconds) {
            Calendar t = periodEnd(context, tolerantDelayMilliseconds);
            return t == null ? null : new Date(t.getTimeInMillis());
        }

        public static Date evaluate(String period) {
            return evaluate(period, null);
        }

        public static Date evaluate(Integer periodSeconds) {
            return evaluate(periodSeconds, null);
        }

        public static Date evaluate(String period,
                                    Integer tolerantDelayMilliseconds) {
            if (period == null || period.isEmpty())
                return null;
            Period p = Period.parse(period);
            if (p == null)
                return null;
            if (tolerantDelayMilliseconds == null) {
                tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(p);
            }
            return eval(p, tolerantDelayMilliseconds);
        }

        public static Date evaluate(Integer periodSeconds,
                                    Integer tolerantDelayMilliseconds) {
            if (periodSeconds == null) {
                return null;
            }
            if (tolerantDelayMilliseconds == null) {
                tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(periodSeconds);
            }
            return eval(Period.secondsOf(periodSeconds),
                    tolerantDelayMilliseconds);
        }

        private static Date eval(Period period, int tolerantDelayMilliseconds) {
            assert period != null;

            Calendar t = period.currentPeriodEnd(tolerantDelayMilliseconds);
            return new Date(t.getTimeInMillis());
        }

    }

    private static Long timeToLong(Calendar t) {
        assert t != null;
        return t.get(Calendar.YEAR) * 10000000000L
                + (t.get(Calendar.MONTH) + 1) * 100000000L
                + t.get(Calendar.DAY_OF_MONTH) * 1000000L
                + t.get(Calendar.HOUR_OF_DAY) * 10000
                + t.get(Calendar.MINUTE) * 100 + t.get(Calendar.SECOND);
    }

    public static class CurPeriodBeginLong extends AbstractUDF {

		private static final long serialVersionUID = -6411179926364436537L;

        public static Long evaluate(OutputContext context) {
            return evaluate(context, null);
        }

        public static Long evaluate(OutputContext context, Integer tolerantDelayMilliseconds) {
            Calendar t = periodBegin(context, tolerantDelayMilliseconds);
            return t == null ? null : timeToLong(t);
        }

        public static Long evaluate(String period) {
			return evaluate(period, null);
		}

		public static Long evaluate(Integer periodSeconds) {
			return evaluate(periodSeconds, null);
		}

		public static Long evaluate(String period,
				Integer tolerantDelayMilliseconds) {
			if (period == null || period.isEmpty())
				return null;
			Period p = Period.parse(period);
			if (p == null)
				return null;
			if (tolerantDelayMilliseconds == null) {
				tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(p);
			}
			return eval(p, tolerantDelayMilliseconds);
		}

		public static Long evaluate(Integer periodSeconds,
				Integer tolerantDelayMilliseconds) {
			if (periodSeconds == null) {
				return null;
			}
			if (tolerantDelayMilliseconds == null) {
				tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(periodSeconds);
			}
			return eval(Period.secondsOf(periodSeconds),
					tolerantDelayMilliseconds);
		}

		private static Long eval(Period period, int tolerantDelayMilliseconds) {
			assert period != null;

			Calendar t = period.currentPeriodBegin(tolerantDelayMilliseconds);
            return timeToLong(t);
		}

	}

    public static class CurPeriodEndLong extends AbstractUDF {

        private static final long serialVersionUID = 1L;

        public static Long evaluate(OutputContext context) {
            return evaluate(context, null);
        }

        public static Long evaluate(OutputContext context, Integer tolerantDelayMilliseconds) {
            Calendar t = periodEnd(context, tolerantDelayMilliseconds);
            return t == null ? null : timeToLong(t);
        }

        public static Long evaluate(String period) {
            return evaluate(period, null);
        }

        public static Long evaluate(Integer periodSeconds) {
            return evaluate(periodSeconds, null);
        }

        public static Long evaluate(String period,
                                    Integer tolerantDelayMilliseconds) {
            if (period == null || period.isEmpty())
                return null;
            Period p = Period.parse(period);
            if (p == null)
                return null;
            if (tolerantDelayMilliseconds == null) {
                tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(p);
            }
            return eval(p, tolerantDelayMilliseconds);
        }

        public static Long evaluate(Integer periodSeconds,
                                    Integer tolerantDelayMilliseconds) {
            if (periodSeconds == null) {
                return null;
            }
            if (tolerantDelayMilliseconds == null) {
                tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(periodSeconds);
            }
            return eval(Period.secondsOf(periodSeconds),
                    tolerantDelayMilliseconds);
        }

        private static Long eval(Period period, int tolerantDelayMilliseconds) {
            assert period != null;

            Calendar t = period.currentPeriodEnd(tolerantDelayMilliseconds);
            return timeToLong(t);
        }

    }

    public static class CurPeriodBeginTime extends AbstractUDF {
		private static final long serialVersionUID = 1L;

        public static Time evaluate(OutputContext context) {
            return evaluate(context, null);
        }

        public static Time evaluate(OutputContext context, Integer tolerantDelayMilliseconds) {
            Calendar t = periodBegin(context, tolerantDelayMilliseconds);
            return t == null ? null : new Time(t.getTimeInMillis());
        }

        public static Time evaluate(String period) {
			return evaluate(period, null);
		}

		public static Time evaluate(Integer periodSeconds) {
			return evaluate(periodSeconds, null);
		}

		public static Time evaluate(String period,
				Integer tolerantDelayMilliseconds) {
			if (period == null || period.isEmpty())
				return null;
			Period p = Period.parse(period);
			if (p == null)
				return null;
			if (tolerantDelayMilliseconds == null) {
				tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(p);
			}
			return eval(p, tolerantDelayMilliseconds);
		}

		public static Time evaluate(Integer periodSeconds,
				Integer tolerantDelayMilliseconds) {
			if (periodSeconds == null) {
				return null;
			}
			if (tolerantDelayMilliseconds == null) {
				tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(periodSeconds);
			}
			return eval(Period.secondsOf(periodSeconds),
					tolerantDelayMilliseconds);
		}

		private static Time eval(Period period, int tolerantDelayMilliseconds) {
			assert period != null;

			Calendar t = period.currentPeriodBegin(tolerantDelayMilliseconds);
			return new Time(t.getTimeInMillis());
		}
	}

    public static class CurPeriodEndTime extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Time evaluate(OutputContext context) {
            return evaluate(context, null);
        }

        public static Time evaluate(OutputContext context, Integer tolerantDelayMilliseconds) {
            Calendar t = periodEnd(context, tolerantDelayMilliseconds);
            return t == null ? null : new Time(t.getTimeInMillis());
        }

        public static Time evaluate(String period) {
            return evaluate(period, null);
        }

        public static Time evaluate(Integer periodSeconds) {
            return evaluate(periodSeconds, null);
        }

        public static Time evaluate(String period,
                                    Integer tolerantDelayMilliseconds) {
            if (period == null || period.isEmpty())
                return null;
            Period p = Period.parse(period);
            if (p == null)
                return null;
            if (tolerantDelayMilliseconds == null) {
                tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(p);
            }
            return eval(p, tolerantDelayMilliseconds);
        }

        public static Time evaluate(Integer periodSeconds,
                                    Integer tolerantDelayMilliseconds) {
            if (periodSeconds == null) {
                return null;
            }
            if (tolerantDelayMilliseconds == null) {
                tolerantDelayMilliseconds = decideTolerantDelayMilliseconds(periodSeconds);
            }
            return eval(Period.secondsOf(periodSeconds),
                    tolerantDelayMilliseconds);
        }

        private static Time eval(Period period, int tolerantDelayMilliseconds) {
            assert period != null;

            Calendar t = period.currentPeriodEnd(tolerantDelayMilliseconds);
            return new Time(t.getTimeInMillis());
        }
    }

	public static class Date_Format extends AbstractUDF {
		private static final long serialVersionUID = 1L;

		public static String evaluate(Date d, String format) {
			if (d == null || format == null)
				return null;
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			return sdf.format(d);
		}
	}

	public static class Time_Format extends AbstractUDF {
		private static final long serialVersionUID = 1L;

		public static String evaluate(Time t, String format) {
			if (t == null || format == null)
				return null;
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			return sdf.format(t);
		}
	}

	public static class Hour extends AbstractUDF {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings("deprecation")
		public static Integer evaluate(Time t) {
			if (t == null)
				return null;
			return t.getHours();
		}
	}

    public static class Minute extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        @SuppressWarnings("deprecation")
        public static Integer evaluate(Time t) {
            if (t == null)
                return null;
            return t.getMinutes();
        }
    }

    public static class Second extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        @SuppressWarnings("deprecation")
        public static Integer evaluate(Time t) {
            if (t == null)
                return null;
            return t.getSeconds();
        }
    }

	public static class AddSeconds extends AbstractUDF {
		private static final long serialVersionUID = 1L;

		public static Date evaluate(Date t, Integer seconds) {
			if (t == null || seconds == null)
				return null;
			return new Date(t.getTime() + seconds * 1000);
		}

		public static Time evaluate(Time t, Integer seconds) {
			if (t == null || seconds == null)
				return null;
			return new Time(t.getTime() + seconds * 1000);
		}
	}

}
