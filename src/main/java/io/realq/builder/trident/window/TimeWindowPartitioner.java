package io.realq.builder.trident.window;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class TimeWindowPartitioner extends BaseFunction {
    private Long window;
    private Long hop;

    private Long baseTime;
    private int hopNo;
    private ConcurrentLinkedQueue<Long> hopTimes = new ConcurrentLinkedQueue<Long>();

    public TimeWindowPartitioner(Long baseTime, Long window, Long hop) {
        super();
        this.baseTime = baseTime;
        this.window = window;
        this.hop = hop;
        this.hopNo = (int) Math.ceil(window / (double) hop);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

        for (int i = 1; i < hopNo + 1; i++) {
            hopTimes.add(baseTime + (hop * i));
        }

        super.prepare(conf, context);
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        Long currentTime = (Long) (System.currentTimeMillis() / 1000);

        for (Long hopTime : hopTimes) {
            // reset window
            if (hopTime <= currentTime - window) {
                hopTimes.add(hopTime + (hop * hopTimes.size()));
                hopTimes.remove(hopTime);

            }

            collector.emit(new Values(hopTime));
        }
    }
}
