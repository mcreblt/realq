package io.realq.builder.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import io.realq.builder.trident.sink.SinkFactory;
import io.realq.parser.element.ElementSink;

public class TridentSink {
    final static Logger logger = LoggerFactory.getLogger(TridentSink.class);
    
    Stream stream;

    public TridentSink(Stream stream) {
        super();
        this.stream = stream;
    }

    public Stream buildStream(ElementSink function, int parallelism) {
               
        stream = SinkFactory.getStream(stream, function.getSinkClass(), function.getInputs(), function.getSinkProperties());      
        
        logger.debug("Building Trident sink: " + function.getSinkClass()
                + "; Stream:" + stream + "; Stream name:" + "; Output fields:" + stream.getOutputFields());
        
        return stream;
    }

    
}
