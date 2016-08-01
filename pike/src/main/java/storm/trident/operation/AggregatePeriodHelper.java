package storm.trident.operation;

import java.util.Calendar;

import com.pplive.pike.base.Period;

class AggregatePeriodHelper {

    static Calendar currentPeriodEnd(Period basePeriod, Period aggregatePeriod) {

        int tolerantMs = secondsErrorTolerance(basePeriod) * 1000;
        return aggregatePeriod.currentPeriodEnd(tolerantMs);

    }

    static Calendar newPeriodEnd(Period basePeriod, Period aggregatePeriod) {
        Calendar t = AggregatePeriodHelper.currentPeriodEnd(basePeriod, aggregatePeriod);
        Calendar now = Calendar.getInstance();
        while (t.before(now) || t.equals(now)) {
            Period.moveCalendarTime(t, aggregatePeriod, 1);
        }
        return t;
    }


    static boolean checkAccumulateEnd(Calendar periodEnd, Period basePeriod) {
		if (periodEnd == null)
			return true;
		
		Calendar t = Calendar.getInstance();
		int tolerance = secondsErrorTolerance(basePeriod);
		t.add(Calendar.SECOND, tolerance);
		return t.after(periodEnd);
	}

	static int secondsErrorTolerance(Period basePeriod) {
        final int seconds = basePeriod.periodSeconds();
        if (seconds <= 60) {
             if (seconds <= 20) {
                 return seconds <= 10 ? 1 : 2;
             }
             else {
                 return seconds <= 30 ? 3 : 5;

             }
        }
        else if (seconds <= 300) {
             return seconds <= 120 ? 10 : seconds <= 180 ? 15 : 30;
        }
        else {
            return 60;
        }
	}
}
