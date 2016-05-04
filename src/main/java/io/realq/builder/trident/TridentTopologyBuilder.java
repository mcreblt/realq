package io.realq.builder.trident;

import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import io.realq.builder.TopologyBuilder;
import io.realq.parser.element.ElementsGraph;
import io.realq.parser.element.ElementsStreamBuilder;

public class TridentTopologyBuilder implements TopologyBuilder {
    final static Logger logger = LoggerFactory.getLogger(TridentTopologyBuilder.class);
    private TridentTopology topology;
    private HashMap<String, Stream> streams;
    
    public TridentTopologyBuilder(TridentTopology topology) {
        super();
        this.topology = topology;
        streams = new HashMap<String, Stream>();
    }

    public TridentTopologyBuilder(TridentTopology topology, HashMap<String, Stream> streams) {
        super();
        this.topology = topology;
        this.streams = streams;
    }
    
    @Override
    public void build(List<ElementsGraph> graphs) {

        for (ElementsGraph graph : graphs) {
            TridentTopologyDetails ttd = new TridentTopologyDetails(this.streams, 
                    this.topology, 
                    graph.getParallelism());
            
            ElementsStreamBuilder streamBuilder = new TridentElementStreamBuilder(
                    ttd, 
                    graph.getStreamName(), 
                    graph.getSourceNames());
            
            graph.setStreamBuilder(streamBuilder);

            graph.build();

        }
        logger.debug("Streams:" + streams);
        for (Stream stream : streams.values()) {
            topology.merge(stream);
        }
    }
}
