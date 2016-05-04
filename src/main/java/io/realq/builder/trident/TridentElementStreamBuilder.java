package io.realq.builder.trident;

import java.util.List;

import storm.trident.Stream;
import io.realq.parser.element.ElementAggregate;
import io.realq.parser.element.ElementFilter;
import io.realq.parser.element.ElementFunction;
import io.realq.parser.element.ElementSink;
import io.realq.parser.element.ElementSource;
import io.realq.parser.element.ElementsStreamBuilder;

public class TridentElementStreamBuilder implements ElementsStreamBuilder {

    private String streamName;
    private Stream currentStream;
    private TridentTopologyDetails ttd;
    public TridentElementStreamBuilder(TridentTopologyDetails ttd,
            String streamName, List<String> sourceName) {
        super();
        this.ttd = ttd;
        this.streamName = streamName;
        if(!sourceName.isEmpty()){
            this.currentStream = ttd.getStream(sourceName.get(0));
        }
    }

    @Override
    public void buildStream(ElementSource element) {
        TridentSource tf = new TridentSource();
        Stream result = tf.buildStream(element, ttd.getTopology(), ttd.getParallelism(), streamName);
        this.currentStream = result;
        ttd.putStream(streamName, result);
    }

    @Override
    public void buildStream(ElementSink element) {
        TridentSink tf = new TridentSink(this.currentStream);
        Stream result = tf.buildStream(element,ttd.getParallelism());
        this.currentStream = result;
        ttd.putStream(streamName, result);
    }
    
    @Override
    public void buildStream(ElementFunction element) {
        TridentFunction tf = new TridentFunction(this.currentStream);
        Stream result = tf.buildStream(element, ttd.getParallelism());
        this.currentStream = result;
        ttd.putStream(streamName, result);
    }

    @Override
    public void buildStream(ElementFilter element) {
        TridentFilter tf = new TridentFilter(this.currentStream);
        Stream result = tf.buildStream(element, ttd.getParallelism());
        this.currentStream = result;
        ttd.putStream(streamName, result);
    }

    @Override
    public void buildStream(ElementAggregate element) {
        TridentAggregate tf = new TridentAggregate(this.currentStream, ttd.getParallelism(), ttd.getTopology());
        Stream result = tf.buildStream(element);
        this.currentStream = result;
        ttd.putStream(streamName, result);
    }


}