package storm.trident.operation;

import java.util.Calendar;

class SlidingState<TState> {
    public final Calendar periodEnd;
    public final TState state;

    public SlidingState(Calendar periodEnd, TState state) {
        this.periodEnd = periodEnd;
        this.state = state;
    }
}
