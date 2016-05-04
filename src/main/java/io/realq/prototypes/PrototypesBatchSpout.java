package io.realq.prototypes;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class PrototypesBatchSpout implements IBatchSpout {

    private int batchSize;
	private int emitedSize;

	public final static String[] VALUES = {"matt", "joanna", "oskar", "aleksander"};

    public PrototypesBatchSpout(int batchSize) throws IOException {
        this.batchSize = batchSize;
    }

    public Values getNextValue() {

        int randomNum = ThreadLocalRandom.current().nextInt((VALUES.length - 1) + 1);
        String value = VALUES[randomNum];

        return new Values(value);
    }
    
    @Override
    public void emitBatch(long batchId, TridentCollector collector) {

    	if(emitedSize < 400){
	        for (int i = 0; i < batchSize; i++) {
	            collector.emit(getNextValue());

	            emitedSize++;
	        }
    	}
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("value");
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
