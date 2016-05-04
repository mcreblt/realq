package io.realq.builder.trident.window;

import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TimeWindowTickSpout implements IBatchSpout {

    private static final long serialVersionUID = 3770132027918065075L;

    private int delay;

    public TimeWindowTickSpout(int delay) {
        this.delay = delay;
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        Utils.sleep(delay);
        long time = (Long) (System.currentTimeMillis() / 1000);

        collector.emit(new Values(time));

    }

    @Override
    public Fields getOutputFields() {
        return new Fields("TIMESTAMP_SECONDS");
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        // TODO Auto-generated method stub

    }

    @Override
    public void ack(long batchId) {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}