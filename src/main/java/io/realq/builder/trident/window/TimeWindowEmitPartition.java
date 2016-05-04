package io.realq.builder.trident.window;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class TimeWindowEmitPartition extends BaseFunction {

    private Long baseTime;
    private Long window;

    public TimeWindowEmitPartition(Long baseTime, long window) {
        super();
        this.baseTime = baseTime;
        this.window = window;

    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        // System.out.println(tuple.get(0));
        // -1 for edge cases where both boundries cross batch
        Long currentTime = tuple.getLong(0) - 1;

        Long boundary = (currentTime - baseTime) % window;

        // return result and is a boundary

        Long previousWindow;
        if (boundary == 0) {
            // - window
            previousWindow = currentTime - this.window;
            collector.emit(new Values(previousWindow));
        } else {
            previousWindow = currentTime - this.window - boundary;
            collector.emit(new Values(previousWindow));
        }

        for (int i = 1; i < window; i++) {
            collector.emit(new Values(previousWindow - i));
        }

    }
}
