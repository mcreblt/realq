package io.realq.builder.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import io.realq.builder.trident.source.SourceFactory;
import io.realq.parser.element.ElementSource;

public class TridentSource {
    final static Logger logger = LoggerFactory.getLogger(TridentSource.class);

    public Stream buildStream(ElementSource function, TridentTopology topology, int parallelism, String streamName) {
        Stream stream = SourceFactory
                .getStream(streamName, topology,
                        function.getSourceClass(),
                        function.getSourceProperties());      
        
        logger.debug("Building Trident source:" + function.getSourceClass()
                + "; Stream:" + stream + "; Stream name:" + streamName + "; Output fields:" + stream.getOutputFields());
        
        return stream;
    }
}
